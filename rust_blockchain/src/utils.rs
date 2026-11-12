use futures::stream::StreamExt;
use tokio_rusqlite::{params, Connection, Result};

use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
use std::env;
use std::fs::File;
use std::io::Write;
use crate::resources;
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
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;

use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};
use crate::crypt;
use prost::Message; 
use crate::block_db;
use crate::bloom;
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::mempool::HEAD_LIST as HEAD_LIST;
use crate::headers;


pub fn get_hash(mut item:Vec::<u8>) -> Vec::<u8>{

    let mut result;
    let mut hasher = Sha256::new();
    hasher.update(&item);
    result = hasher.finalize().to_vec();
    let encoded = hex::encode(&result);
    return result;
    
}


pub fn check_merkle(mut proof: Vec::<Vec::<u8>>) -> bool{
    ////////println!("Checking merkle proof");

    for i in (0..proof.len()-1).step_by(2) {
        let combined = [proof[i].clone(), proof[i+1].clone()].concat();
        let mut hasher = Sha256::new();
        hasher.update(&combined);
        let result = hasher.finalize().to_vec();
        if result == proof[i+2] || result == proof[i+3]{
            let encoded = hex::encode(&result);
        }
        else{
            return false;
        }
        if i >= proof.len()-1{
            return true;
        }
        
    }
    ////////println!("Merkle tree valid");
    return true;
}



pub async fn check_resource(grid: String) -> Result<(bool)>{
    let mut commit_list = mempool::MEMPOOL.read().await;
    ////println!("Checking mempool");
    for item in commit_list.iter(){
        if item.payload.as_ref().unwrap().grid == grid{
            //println!("Found in mempooo");
            return Ok(true)
        }
    }
    let mut header_list = headers::HeaderList.read().await;
    for header in header_list.iter(){
        let bloom_match = header.check_bloom(grid.clone());
        ////println!("QUICL {}", bloom_match);
        let encoded = hex::encode(header.merkle_root.clone());
        let grid_clone = grid.clone();
        let block_load = task::spawn(async move {
            return block_db::check_resource(grid_clone, format!("{encoded}.db"));
        });
        match block_load.await {
            Ok(result) => {
                
                let db_result = result.await;
                if db_result.unwrap() == true{
                    //println!("Found in database");
                    return Ok(true);
                }
            }
            Err(err) => println!("Task panicked or was cancelled: {:?}", err)
        }
    }
 
    Ok(false)
}


pub fn check_merkle_proof(proof: Vec::<Vec::<u8>>, resource: Vec::<u8>) -> bool{
    let mut current: Vec::<u8> = resource;
    for pair in proof.chunks(2) {
        if let [first, second] = pair {
            if current == *first || current == *second{
            }
            else{
                return false;
            }
            let mut combined_hash = first.clone();
            combined_hash.extend(second.clone());
            let mut final_hasher = Sha256::new();
            final_hasher.update(&combined_hash);
            current = final_hasher.finalize().to_vec();
        }
    }
    if current == *proof.last().unwrap(){
        return true;
    }
    return false;
}

pub fn xor_bit_distance(a: &[u8], b: &[u8]) -> u32 {
    // Optional: runtime verification to ensure inputs are 32 bytes
    debug_assert_eq!(a.len(), 32);
    debug_assert_eq!(b.len(), 32);

    a.iter()
     .zip(b.iter())
     .map(|(x, y)| (x ^ y).count_ones())
     .sum()
}


pub async fn create_dump(){
    let commit_global_list = resources::ResourcePool.write().await;
    println!("Starting dump!");
    let args: Vec<String> = env::args().collect();
    let db_dir = args[2].clone();
    let dump_file_name = format!("{db_dir}/dump.txt");
    let mut file = File::options()
        .append(true)
        .create(true) // Optional: creates the file if it's missing
        .open(dump_file_name)
        .unwrap();
    writeln!(file, "NEW DUMP");
    writeln!(file, "MEMPOOL");
    for resource in commit_global_list.iter(){
        writeln!(file, "{}", resource.payload.as_ref().unwrap().grid);
    }

}