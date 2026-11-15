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
    pub async fn propose_block(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        println!("Proposing new block");
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return,
        };


        let new_header = headers::Header::generate_header(election.identifier.clone()).await;
        let merkle_encoded = hex::encode(new_header.merkle_root.clone());
        let args: Vec<String> = env::args().collect();
        let db_dir = args[2].clone();

        let mut block_buf = Vec::<u8>::with_capacity(32);
        let mut body_buf = Vec::<u8>::new();

        new_header.to_protobuf().encode(&mut body_buf).unwrap();
        let signed_header = self_node::sign_resource(body_buf.clone()).await;


        let new_block = proto_bufs::Block{
            body: Some(new_header.to_protobuf()),
            resources: new_header.resources.clone(),
            body_signature: signed_header.clone()
        };
        new_block.encode(&mut block_buf).unwrap();


        let mut buf = Vec::<u8>::with_capacity(32);
        let vote_message = proto_bufs::ConsensusMessage{
            r#type: String::from("PROPOSE"),
            identifier: election.identifier.clone(),
            message: block_buf,
            signature: Vec::new(),
            geid: crate::get_geid().to_string(),
            key: self_node::sign_key().await.clone()
        };
        vote_message.encode(&mut buf).unwrap();
        let vote_chainmessage = proto_bufs::ChainMessage{
            r#type: String::from("CONSENSUS"),
            message: buf.clone()
        };
        ////println!("Broadcasting proposed block of size: {}", buf.len());
        if tx_msg.send(vote_chainmessage.clone()).await.is_err() {}
        election.votes = 1;
        election.proposed = Some(new_header);
        drop(election_list);
    }




    pub async fn validate_block(index: usize, consensus_message: proto_bufs::ConsensusMessage, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        let header = proto_bufs::Block::decode(&(consensus_message.message.clone())[..]).unwrap();
        let mut loaded_header = headers::Header::block_to_head(header.clone()).await;
        loaded_header.key = consensus_message.key.clone().unwrap().payload.as_ref().unwrap().grid.clone();
        loaded_header.signature = header.body_signature.clone();
        let body = header.body.clone().unwrap();
        println!("Recieved proposed: {}", hex::encode(body.merkle_root));
        let mut resources_validity = true;


        let header_validity = headers::Header::check_header(header.clone()).await;
        if !header_validity{
            //return
        }

        loaded_header.merkle_tree =  headers::Header::generate_merkle(header.resources.clone()).clone();

        let mut vote: Vec::<u8>;
        if resources_validity{
            vote = vec![1];
        }
        else{
            vote = vec![0];
            //return;
        }
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return,
        };
        election.proposed = Some(loaded_header);
        let signed_commit = self_node::sign_resource(vote.clone()).await;
        let mut buf = Vec::<u8>::with_capacity(32);
        let vote_message = proto_bufs::ConsensusMessage{
            r#type: String::from("VALIDATE"),
            identifier: election.identifier.clone(),
            message: vote,
            signature: Vec::new(),
            geid: crate::get_geid().to_string(),
            key: self_node::sign_key().await.clone()
        };
        vote_message.encode(&mut buf).unwrap();
        let vote_chainmessage = proto_bufs::ChainMessage{
            r#type: String::from("CONSENSUS"),
            message: buf.clone()
        };

        if tx_msg.send(vote_chainmessage.clone()).await.is_err() {}
        election.votes = election.votes + 1;
        election.validated = true;
        drop(election_list);
    }
    pub async fn add_validation(index: usize, consensus_message: proto_bufs::ConsensusMessage, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return,
        };
        let mut node_match: Option<&mut Node> = None;

        for node in election.nodes.iter_mut(){
            if node.geid == consensus_message.geid{
                node_match = Some(&mut *node);
                break;
            }
        }
        if node_match.is_none(){
            return
        }
        let node_match = node_match.unwrap();
        if node_match.validated{
            return
        }
        node_match.validated = true;
        election.votes = election.votes + 1;


        if election.votes > ((election.nodes.len() as i32))/2{
            ////println!("Proposed  block confirmed!");
            if !election.finalized{
                let head_saved = headers::Header::add_block_db(election.proposed.clone().unwrap()).await;
                let blocks_saved = headers::Header::save_resources(election.proposed.clone().unwrap()).await;
                election.finalized = true;
                election_list.clear();
            }
            
        }
        drop(election_list);
    }
}