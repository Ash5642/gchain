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
use crate::mempool::BLOCKHEAD as BLOCKHEAD;

use crate::mempool::PROCESS as PROCESS;
use crate::mempool;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};
use prost::Message; 
use crate::block_db;
use fastbloom::BloomFilter;

#[derive(Clone)]
#[derive(Debug)]

struct MerkleNode{
    hash: Vec::<u8>,
    leaf:Option<Box<MerkleNode>>,
    parent: Option<Box<MerkleNode>>,
    children: Vec::<Box<MerkleNode>>,
}


pub async fn create_bloom(mut resources:Vec::<Resource>) -> mempool::BLOOMFILTER{
    let my_seed = 12345u128; 
    let mut filter = BloomFilter::with_false_pos(0.01)
        .seed(&12345u128)
        .expected_items(1000); // Correctly constructs the BloomFilter<DefaultHasher>

    for item in resources.clone(){
        //////////////println!("ADDING ITEM {} TO FILTER", item.clone().payload.unwrap().grid);
        if let Some(inner) = &item.payload {
            let name_clone = inner.grid.clone();
            filter.insert(&name_clone);
            assert!(filter.contains(&name_clone));
        }
    }
    let saved_bits: Vec<u64> = filter.as_slice().to_vec();
    let num_hashes = filter.num_hashes();

    let new_filter = mempool::BLOOMFILTER{
        bits: saved_bits.clone(),
        num_hashes:num_hashes.clone()
    };
    return new_filter
}



pub fn check_header(mut grid:String, header:BLOCKHEAD) -> bool{

        ////////////////println!("CHECKING BLOOM for {}", grid.clone());
    let restored_filter = BloomFilter::from_vec(header.bloom.bits.clone())
        .seed(&12345u128)
        .hashes(header.bloom.num_hashes.clone());
    let contains = restored_filter.contains(&grid);
    ////////////////println!("contains {}:{}", contains, grid);
    let grid_cloned = grid.clone();
    let encoded = hex::encode(header.merkle.clone());
    return contains;
        
    
    ////////////////println!("NO CHECKING BLOOM");
    
}