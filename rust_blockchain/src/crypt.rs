use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
use crate::mempool_actions;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Mutex, OnceLock};
use tokio::task;
use rand::prelude;
use rand::Rng;
use std::error::Error;
use std::time::Duration;
use crate::mempool::MEMPOOL as MEMPOOL;
use crate::mempool::MESSAGE_POOL as MESSAGE_POOL;
use crate::mempool::PROCESS_POOL as PROCESS_POOL;
use crate::mempool::PROCESS_QUEUE as PROCESS_QUEUE;
use crate::mempool::PROCESS as PROCESS;
use crate::mempool;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use crate::resources;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::ChainMessage as ChainMessage;
use crate::proto_bufs::Header as Header;
use crate::proto_bufs::BloomFilter as BloomFilter;

use sha2::{Sha256, Digest};
use prost::Message; 
use crate::block_db;
use crate::bloom;


#[derive(Clone)]
#[derive(Debug)]

pub struct MerkleNode{
    pub hash: Vec::<u8>,
    pub leaf:Option<String>,
    pub parent: Option<Vec::<u8>>,
    pub children: Vec::<Box<MerkleNode>>,
}



pub async fn create_merkle() -> Option<Header>{
    let root = MerkleNode{
        hash: Vec::new(),
        leaf:None,
        parent:None,
        children:Vec::new()
    };
    let mut nodes: Vec::<Vec::<MerkleNode>> = Vec::new();
    let mut commit_list = resources::ResourcePool.write().await;
    //println!("Confirming {} resources", commit_list.len());
    let mut depth:u32 = 2;
    let base: i32 = 2;
    while base.pow(depth+1) <commit_list.len().try_into().unwrap(){
        depth = depth+1;
    }
    nodes.push(
        Vec::new()
    );
    let mut resources_included:Vec<Resource> = Vec::new();
    for i in 0..(base as usize).pow(depth as u32){
        let mut buf = Vec::new();
        resources_included.push(commit_list[i].clone());
        commit_list[i].payload.clone().unwrap().encode(&mut buf);
        let mut hasher = Sha256::new();
        hasher.update(&buf);
        let result = hasher.finalize().to_vec();
    
        if let Some(inner) = &commit_list[i].payload {
            ////////////////println!("Nested field value: {}", inner.grid);
            let name_clone = inner.grid.clone();

        }
        let leaf = MerkleNode{
            hash: result.clone(),
            leaf:Some((commit_list[i].clone().payload).unwrap().grid),
            parent:None,
            children:Vec::new()
        };
        nodes[0].push(leaf);
        let encoded = hex::encode(&result);
        ////////////////println!("hash {}: {}", i, encoded);   
    }
    
    commit_list.drain(0..(base as usize).pow(depth as u32));
    //println!("Agter size: {}", commit_list.len());
    
    
    
    
    while true{
        if let Some(top_wrapped) = nodes.last_mut() {
            let mut top = (*top_wrapped).clone();
            if top.clone().len() == 1{
                break;
            }
            let mut new_level:Vec::<MerkleNode> = Vec::new();
            for i in 0..top_wrapped.clone().len()/2{
                let mut combined_hash:Vec::<u8> = Vec::new();
                combined_hash.extend(&top[i*2].hash);
                combined_hash.extend(&top[(i*2)+1].hash);
                let mut hasher = Sha256::new();
                hasher.update(&combined_hash);
                let result = hasher.finalize().to_vec();
                let new_node = MerkleNode {
                    hash: result.clone(),
                    leaf:None,
                    parent:None,
                    children:Vec::new(),
                };
                top_wrapped[i*2].parent = Some(result.clone());
                top_wrapped[(i*2)+1].parent = Some(result.clone());
                new_level.push(new_node);
            }
            nodes.push(new_level);
            

        }
    }
    let root_encoded = hex::encode(&nodes.last_mut().unwrap()[0].hash);
    //////println!("Root: {}", root_encoded);
    if let Some(top_wrapped) = nodes.last() {
        let top = (*top_wrapped).clone();
        let encoded = hex::encode(&top[0].hash);
        ////////////////println!("{}", encoded);
        let resources_included_clone = resources_included.clone();
        let db_handler = task::spawn(async move {
            block_db::create_block_db(resources_included, encoded, nodes.clone()).await;
        });
        
    
        let bloom_handler = task::spawn(async move {
            return bloom::create_bloom(resources_included_clone).await;
        });
        match bloom_handler.await {
            Ok(result) => {
                ////////////////println!("Task returned: {:?}", result);
                let mut header_list = mempool::HEAD_LIST.write().await;
                let new_head = mempool::BLOCKHEAD{
                    bloom: result.clone(),
                    merkle:top[0].clone().hash
                };
                let new_head_cloned = new_head.clone();
                task::spawn(async move {
                    block_db::add_block_db(new_head_cloned).await;
                });
                header_list.push(new_head.clone());
                let header_protobuf = Header{
                    stake: String::from(""),
                    num: 0,
                    previous: Vec::new(),
                    bloom: Some(BloomFilter{
                        filter: Vec::new(),
                        num_hashes: 3
                    }),
                    key: "".to_string(),
                    state_root: Vec::new(),
                    merkle_root:top[0].clone().hash
                };
                return Some(header_protobuf.clone())
            }
            Err(err) => println!("Task panicked or was cancelled: {:?}", err)
        }
    }
    return None
    

}