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
use crate::blocks;
use crate::resources::validators;
use std::fs;
use crate::blocks::merkle::MerkleNode;




use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use crate::resources;
use rand::RngExt;
use rand::Rng;
use sha2::{Sha256, Digest};
use crate::self_node;
use std::env;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;
use crate::resources::signatures;

use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};



impl Election{
     pub async fn reveal(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) {
        //println!("Revealing...");
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return, // Safely handle out-of-bounds index
        };
        if election.revealed{
            return
        }
        let mut self_node: Option<Node> = None;

        for node in election.nodes.iter(){
            if node.geid == crate::get_geid(){
                self_node = Some(node.clone());
                break;
            }
        }
        if self_node.is_none(){
            return
        }
        let self_node = self_node.unwrap().clone();
        let mut combined = Vec::with_capacity(self_node.value.len() + self_node.nonce.len());
        combined.extend_from_slice(&self_node.value);
        combined.extend_from_slice(&self_node.nonce);
        let signed_commit = self_node::sign_resource(combined.clone()).await;
        let mut buf = Vec::<u8>::with_capacity(32);
        let vote_message = proto_bufs::ConsensusMessage{
            r#type: String::from("REVEAL"),
            identifier: election.identifier.clone(),
            message: combined,
            signature: signed_commit.clone(),
            geid: crate::get_geid().to_string(),
            key: self_node::sign_key().await.clone()
        };
        vote_message.encode(&mut buf).unwrap();
        let vote_chainmessage = proto_bufs::ChainMessage{
            r#type: String::from("CONSENSUS"),
            message: buf.clone()
        };
        election.revealed = true;
        if tx_msg.send(vote_chainmessage.clone()).await.is_err() {}
    }
    pub async fn add_reveal(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>, consensus_message: proto_bufs::ConsensusMessage) {
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return, // Safely handle out-of-bounds index
        };
        let mut node_match: Option<&mut Node> = None;

        for node in election.nodes.iter_mut(){
            if node.geid == consensus_message.geid{
                node_match = Some(&mut *node);
                break;
            }
        }
        if node_match.is_none(){
            //return
        }
        
        let node_match = node_match.unwrap();

        let node_key = consensus_message.clone().key.unwrap().clone();
        let key_validity = validators::verify_resource(&node_key, None).await;
        let node_key_loaded = signatures::load_key(&node_key).unwrap();
        let signature_valid = node_key_loaded.check_signature_bytes(consensus_message.signature.clone(), consensus_message.message.clone());

        let mut include = true;
        if  !signature_valid || key_validity.is_none() || key_validity.as_ref().unwrap().as_str() != "CHAIN"{
            include = false;
        }
        if !include{
            //return;
        }
    
        node_match.value = consensus_message.message[..32].to_vec();
        node_match.nonce = consensus_message.message[32..].to_vec();
    }
}