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
    pub async fn propose_block(identifier: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        let new_block = blocks::Header::generate_header(identifier).await;
        println!("New block generated");
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(identifier) {
            Some(e) => e,
            None => return
        };
        let mut self_node = match election.nodes.get_mut(crate::get_geid()){
            Some(n) => n,
            _ => return,
        };

        let block_encoded = new_block.to_bytes();
        self_node.proposed = Some(new_block);
        Election::send_consensus_message(identifier, "PROPOSE", &block_encoded, tx_msg).await;  
        election.stage = 3;
        println!("Proposed!");
        
    }

    pub async fn validate_block(index: &Vec::<u8>, consensus_message: proto_bufs::ConsensusPayload, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        println!("Validating block!");
    }

}