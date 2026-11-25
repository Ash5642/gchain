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
use std::sync::atomic::Ordering;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};



impl Election{
     pub async fn reveal(index: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) {
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return,
        };
        if election.stage > 1{
            return
        }
        let mut self_node = match election.nodes.get_mut(crate::get_geid()){
            Some(n) => n,
            _ => return,
        };

        
        let mut combined = Vec::new();
        combined.extend_from_slice(&self_node.value.unwrap());
        combined.extend_from_slice(&self_node.commit_nonce.unwrap());
        let signed_commit = self_node::sign_resource(combined.clone()).await;
        Election::send_consensus_message(index, "REVEAL", &combined, tx_msg).await;

        election.stage = 2;
    }


    
    pub async fn add_reveal(index: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>, consensus_message: proto_bufs::ConsensusPayload) {
        let mut election_list = ElectionList.write().await;

        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return, // Safely handle out-of-bounds index
        };
        let mut node_match = match election.nodes.get_mut(&consensus_message.geid){
            Some(n) => n,
            _ => return,
        };

        let hashed_reveal = utils::get_ref_hash(&consensus_message.message);
        if node_match.commit != *hashed_reveal{
            println!("Wrong value revealed!");
            return;
        }
        node_match.value = Some(consensus_message.message[..32].try_into().expect("Size!"));
        node_match.commit_nonce = Some(consensus_message.message[32..].try_into().expect("Size!"));
        println!("Reveal from: {}", consensus_message.geid);
        
    }
}