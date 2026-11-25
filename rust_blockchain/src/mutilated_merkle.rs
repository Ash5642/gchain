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


pub struct MerkleNode{
    pub value: [u8; 32],
    pub children: [Option<usize>; 2],
    pub resource_count : u32,
    pub ghost_count : u32,
    pub is_resource: bool
}


pub struct MerkleTree{
    pub nodes: Vec::<MerkleNode>,
}

impl MerkleTree{
    pub fn create() -> MerkleTree{
        let new_root = MerkleNode{
            value: [0u8; 32],
            children: [None, None],
            ghost_count: 0,
            resource_count: 0,
            is_resource: false
        };
        return MerkleTree{
            nodes: vec![new_root]
        }
    }

    pub fn add_resource(mut self, resource: Vec::<u8>) -> Self{
        let resource_depth = rand::random_range(3..50);
        println!("Filling at depth {}", resource_depth);
        self.walk(0, resource_depth, &resource);
        self
    }

    pub fn walk(&mut self, current: usize, depth: i32, resource: &Vec::<u8>) -> bool{
        if depth == 0{
            println!("Written!");
            return true;
        }
        let resource_depth = rand::random_range(3..50);
        let current_node = match self.nodes.get(current){
            Some(n) => n,
            None => return false
        };
        let mut branch: usize = rand::random_range(0..2);
        let mut next_node = self.get_or_insert(current_node.children[branch]);
        let mut branch_result = self.walk(next_node, depth-1, resource);
        let mut current_node = match self.nodes.get_mut(current){
            Some(n) => n,
            None => return false
        };
        current_node.children[branch] = Some(next_node);
        true
    }

    

    pub fn get_or_insert(&mut self, current_pointer: Option<usize>) -> usize{
        if let Some(current) = current_pointer{
            println!("Points to existing node {}", current);
            return current
        }
        return self.insert_node(false);
    }

    pub fn insert_node(&mut self, resource: bool) -> usize{
        self.nodes.push(
            MerkleNode{
                value: [0u8; 32],
                children: [None, None],
                ghost_count: 0,
                resource_count: 0,
                is_resource: resource
            }
        );
        return self.nodes.len() -1;
    }


}