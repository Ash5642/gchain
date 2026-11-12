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
        println!("CP1");
        let value: Vec::<u8> = String::from("ass").into_bytes();
        let nonce: Vec::<u8> = String::from("cock").into_bytes();
        
        
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
        println!("CP2");
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
        println!("CP3");
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
        println!("Commit successful!");
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

    pub async fn reveal(index: usize, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) {
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
        let key_validity = validators::verify_resource(node_key.clone(), None).await;
        let node_key_loaded = signatures::load_key(node_key).unwrap();
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
        println!("{:#?}", election.nodes);
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
            println!("\nNode: {}\nDistance:{}", node.geid, distance);
        }
    }


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
        println!("Broadcasting proposed block of size: {}", buf.len());
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
            println!("Proposed  block confirmed!");
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




