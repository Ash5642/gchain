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
    pub async fn set_target(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return,
        };
        let mut target = vec![0u8; 32];
        rand::rng().fill_bytes(&mut target);
        election.target = target.clone();
        let mut buf = Vec::<u8>::with_capacity(32);
        let vote_message = proto_bufs::ConsensusMessage{
            r#type: String::from("TARGET"),
            identifier: election.identifier.clone(),
            message: target.clone(),
            signature: Vec::new(),
            geid: crate::get_geid().to_string(),
            key: None
        };
        vote_message.encode(&mut buf).unwrap();
        let vote_chainmessage = proto_bufs::ChainMessage{
            r#type: String::from("CONSENSUS"),
            message: buf.clone()
        };
        election.revealed = true;
        let tx_msg_win_clone = tx_msg.clone();
        if tx_msg.send(vote_chainmessage.clone()).await.is_err() {}
        drop(election_list);
        Election::add_target(index, target.clone(), tx_msg_win_clone).await;
    }

    pub async fn add_target(index: usize, target: Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) -> bool{
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return false,
        };
        election.target = target.clone();
        let mut closest_distance = 256;
        let mut closest_match: Option<Node> = None;
        ////println!("{:#?}", election.nodes);
        for node in election.nodes.iter(){
            let distance = utils::xor_bit_distance(&node.value[..], &target[..]);
            if distance < closest_distance{
                closest_match = Some(node.clone());
                closest_distance = distance;
            }
        }
        println!("Picked validator: {}", closest_match.clone().unwrap().geid);
        election.validator = Some(closest_match.clone().unwrap().geid);
        drop(election_list);
        if closest_match.clone().unwrap().geid == crate::get_geid(){
            Election::propose_block(index, tx_msg).await;
        }
        return false
    }

    pub async fn distance(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) {
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return,
        };
        let mut target = vec![0u8; 32];
        rand::rng().fill_bytes(&mut target);
        for node in election.nodes.iter(){
            let distance = utils::xor_bit_distance(&node.value[..], &target[..]);
            ////println!("\nNode: {}\nDistance:{}", node.geid, distance);
        }
    }
}