use super::Election as Election;
use super::ElectionList as ElectionList;

use super::Node as Node;

use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;
use crate::proto_bufs;
use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;
use prost::Message;
use crate::headers;
use crate::validators;
use std::fs;
use crate::crypt::MerkleNode as MerkleNode;
use crate::mempool::BLOCKHEAD as BLOCKHEAD;
use crate::mempool::DBHead as DBHead;
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::mempool::HEAD_LIST as HEAD_LIST;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use crate::resources::ChainResource;
use rand::RngExt;
use rand::Rng;
use sha2::{Sha256, Digest};
use crate::self_node;
use std::env;
use crate::blocks;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;
use crate::signatures;
use crate::cash;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};



impl Election{
    pub async fn commit(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) {
        println!("Committing...");
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return, // Safely handle out-of-bounds index
        };
        if election.commited {
            return;
        }
        ////println!("CP1");
        let value: Vec::<u8> = String::from("test").into_bytes();
        let nonce: Vec::<u8> = String::from("test_nonce").into_bytes();
        
        
        let mut value = vec![0u8; 32];
        let mut nonce = vec![0u8; 32];
        rand::rng().fill_bytes(&mut value);
        rand::rng().fill_bytes(&mut nonce);


        let mut combined = Vec::with_capacity(value.len() + nonce.len());
        combined.extend_from_slice(&value);
        combined.extend_from_slice(&nonce);

        let mut result;
        let mut hasher = Sha256::new();

        

        let mut commit_buf = Vec::<u8>::with_capacity(32);
        let commit_payload = proto_bufs::CommitMessage{
            hash: value.clone(),
            stakes: vec![]
        };
        commit_payload.encode(&mut commit_buf).unwrap();

        hasher.update(&combined);
        result = hasher.finalize().to_vec();
        let args: Vec<String> = env::args().collect();
        let db_dir = args[2].clone();
        ////println!("CP2");
        let signed_commit = self_node::sign_resource(commit_buf.clone()).await;
        election.nodes.push(
            Node{
                node: None,
                commit: result.clone(),
                value: value.clone(),
                geid: crate::get_geid().to_string(),
                nonce: nonce,
                validated: false,
                stakes: vec![]
            }
        );
        ////println!("CP3");
        let mut buf = Vec::<u8>::with_capacity(32); 
        let vote_message = proto_bufs::ConsensusMessage{
            r#type: String::from("COMMIT"),
            identifier: election.identifier.clone(),
            message: commit_buf.clone(),
            signature: signed_commit.clone(),
            geid: crate::get_geid().to_string(),
            key: self_node::sign_key().await.clone()
        };
        vote_message.encode(&mut buf).unwrap();
        let vote_chainmessage = proto_bufs::ChainMessage{
            r#type: String::from("CONSENSUS"),
            message: buf.clone()
        };
        election.commited = true;
        if tx_msg.send(vote_chainmessage.clone()).await.is_err() {}
        ////println!("Commit successful!");
    }
    pub async fn add_commit(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>, sender: PeerId, consensus_message: proto_bufs::ConsensusMessage) {
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => {
                return
            }, // Safely handle out-of-bounds index
        };
        let mut exists = false;
        for node in election.nodes.iter(){
            if node.geid.clone() == consensus_message.geid.clone(){
                exists = true;
            }
        }
        let commit_payload = proto_bufs::CommitMessage::decode(&(consensus_message.message.clone())[..]).unwrap();

        let node_key = consensus_message.clone().key.unwrap().clone();
        let key_validity = validators::verify_resource(node_key.clone(), None).await;
        let node_key_loaded = signatures::load_key(node_key).unwrap();
        let signature_valid = node_key_loaded.check_signature_bytes(consensus_message.signature.clone(), consensus_message.message.clone());
        

        let mut include = true;
        if exists || !signature_valid || key_validity.is_none() || key_validity.as_ref().unwrap().as_str() != "CHAIN"{
            include = false;
        }

        if !include{
            //return;
        }

        election.nodes.push(
            Node{
                node: Some(sender),
                commit: commit_payload.hash.clone(),
                value: Vec::with_capacity(32),
                geid: consensus_message.geid.clone(),
                nonce: Vec::with_capacity(32),
                validated: false,
                stakes: commit_payload.stakes.clone()
            }
        );
    }
}