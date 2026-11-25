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
use tokio::task::{AbortHandle, JoinHandle};
mod commit;
mod propose;
mod elect;
mod reveal;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
use std::sync::atomic::{AtomicBool, Ordering};
struct ElectionReceivers{
    commit: Receiver<proto_bufs::ChainMessage>,
    reveal: Receiver<proto_bufs::ChainMessage>,
}
use std::collections::HashMap;

#[derive(Clone)]
#[derive(Debug)]
pub struct Node{
    pub commit: [u8; 32],
    pub stake: u64,
    pub commit_nonce: Option<[u8; 32]>,
    pub value: Option<[u8; 32]>,
    pub reveal_nonce: Option<[u8; 32]>,
    pub proposed: Option<blocks::Header>,
    pub node: Option<PeerId>,
    pub geid: String,
    
}

pub struct Election{

    pub PoW_enabled: bool,
    pub recievers: Option<ElectionReceivers>,
    pub identifier: Vec::<u8>,
    pub nodes: HashMap::<String, Node>,
    pub stage: u8,
    pub target: Vec::<u8>,
    pub stop_grind_flag: Arc<AtomicBool>,
    pub staked: u64
}



pub static ElectionList: LazyLock<Arc<RwLock<HashMap<Vec::<u8>, Election>>>> = LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

impl Election{
    pub async fn start_election(identifier: &Vec::<u8>) -> bool{
        let mut election_list = ElectionList.read().await;
        if let Some(election) = election_list.get(identifier){
            return true
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
            PoW_enabled: false,
            stage: 0,
            staked: 0,
            recievers: Some(recievers),
            identifier: identifier.clone(),
            nodes: HashMap::new(),
            target: Vec::with_capacity(32),
            stop_grind_flag: Arc::new(AtomicBool::new(false)),
        };
        election_list.insert(identifier.to_vec(), election);
        true
    }

    pub async fn send_consensus_message(identifier: &Vec::<u8>, r#type: &str, message: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){

        let consensus_payload = proto_bufs::ConsensusPayload{
            r#type: r#type.to_string(),
            identifier: identifier.clone(),
            message: message.clone(),
            geid: crate::get_geid().to_string(),
            key: self_node::sign_key().await.clone()
        };
        let mut consensus_payload_encoded = Vec::new();
        consensus_payload.encode(&mut consensus_payload_encoded);
        let consensus_message = proto_bufs::ConsensusMessage{
            payload: Some(consensus_payload),
            signature: self_node::sign_resource(consensus_payload_encoded.clone()).await,
        };
        let mut consensus_message_encoded = Vec::new();
        consensus_message.encode(&mut consensus_message_encoded);
        let vote_chainmessage = proto_bufs::ChainMessage{
            r#type: String::from("CONSENSUS"),
            message: consensus_message_encoded.clone()
        };
        if tx_msg.send(vote_chainmessage.clone()).await.is_err() {}

    }
    pub async fn check_consensus_message(consensus_message: &proto_bufs::ConsensusMessage) -> bool{
        let election_list = ElectionList.read().await;
        let consensus_payload = match consensus_message.payload.as_ref(){
            Some(cp) => cp,
            None => {
                println!("Consensus message payload not included!");
                return false;
            }
        };

        let node_key = match &consensus_payload.key{
            Some(k) => k,
            None => {
                println!("Consensus key not included!");
                return false;
            }
        };
        let key_validity = validators::verify_resource(&node_key.clone(), None).await;
        let node_key_loaded = signatures::load_key(&node_key).unwrap();

        let mut consensus_payload_encoded = Vec::new();
        consensus_payload.encode(&mut consensus_payload_encoded);

        let signature_valid = node_key_loaded.check_signature_bytes(consensus_message.signature.clone(), consensus_payload_encoded.clone());
        println!("Signature: {}", signature_valid);
        
        true
        
    }



}




