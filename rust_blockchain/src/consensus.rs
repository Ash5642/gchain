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
mod commit;
mod propose;
mod elect;
mod reveal;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};

struct ElectionReceivers{
    commit: Receiver<proto_bufs::ChainMessage>,
    reveal: Receiver<proto_bufs::ChainMessage>,
}
#[derive(Clone)]
#[derive(Debug)]
pub struct Node{
    pub node: Option<PeerId>,
    pub value: Vec::<u8>,
    pub nonce: Vec::<u8>,
    pub commit: Vec::<u8>,
    pub geid: String,
    pub validated: bool,
    pub stakes: Vec::<String>
}

pub struct Election{
    pub commited: bool,
    pub revealed: bool,
    pub validated: bool,
    pub recievers: Option<ElectionReceivers>,
    pub identifier: Vec::<u8>,
    pub nodes: Vec::<Node>,
    pub stage: i32,
    pub target: Vec::<u8>,
    pub votes: i32,
    pub validator: Option<String>,
    pub proposed: Option<headers::Header>,
    pub finalized: bool
}



pub static ElectionList: LazyLock<Arc<RwLock<Vec<Election>>>> = LazyLock::new(|| Arc::new(RwLock::new(vec![])));

impl Election{
    pub async fn start_election(identifier: Option<Vec::<u8>>) -> usize{
        let mut election_list = ElectionList.read().await;
        for (election_index, election) in election_list.iter().enumerate(){
            if election.identifier == identifier.clone().unwrap(){
                return election_index
            }
        }
        
        drop(election_list);
        
        let mut election_list = ElectionList.write().await;
        let (tx_commit, mut rx_commit) = mpsc::channel(100);
        let (tx_reveal, mut rx_reveal) = mpsc::channel(100);
        let recievers = ElectionReceivers{
            commit: rx_commit,
            reveal: rx_reveal
        };
        let election = Election{
            commited: false,
            revealed: false,
            validated: false,
            recievers: Some(recievers),
            identifier: identifier.clone().unwrap(),
            nodes: Vec::new(),
            target: Vec::with_capacity(32),
            stage: 0,
            votes: 0,
            validator: None,
            proposed: None,
            finalized: false
        };
        election_list.push(election);
        println!("Starting election: {:?}", identifier.unwrap());
        election_list.len() - 1
    }





}




