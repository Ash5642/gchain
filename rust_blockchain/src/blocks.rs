use tokio_rusqlite::{params, Connection, Result};
use std::collections::HashSet;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs;
use std::fs;

use tokio::sync::mpsc;
use crate::blocks;
use crate::state;
use crate::resources;
use crate::utils;
use crate::entity;
use fastbloom::BloomFilter;
use sha2::{Sha256, Digest};
use prost::Message; 
use crate::vm;



use std::path::Path;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;

use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;




use std::env;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;


mod execution;
mod generation;
pub mod merkle;
mod storage;
mod bloom;
mod storage_read;
use crate::blocks::merkle::MerkleNode;
#[derive(Debug)]
struct DBResource {
    gid: String,
    action: String
}
#[derive(Debug)]
struct DBField {
    key: String
}
#[derive(Debug)] 
pub struct DBResult {
    resource: DBResource,
    fields: Vec<DBField>
}
#[derive(Clone)]
#[derive(Debug)] 

pub struct Header{
    pub num:u32,
    pub key: String,
    pub signature: Vec::<u8>,
    pub merkle_root:Vec::<u8>,
    pub state_root: Vec::<u8>,
    pub bloom:Option<BloomFilter>,
    pub db_conn: Option<Connection>,
    pub previous_hash: Option::<Vec::<u8>>,
    pub resources_total: i32,
    pub resources: Vec::<Resource>,
    pub merkle_tree: Vec::<Vec::<MerkleNode>>,
    pub state: Option<state::StateSession>,
    pub grind_nonce: Option<[u8; 32]>
}


pub static HeadersList: LazyLock<Arc<RwLock<Vec<Header>>>> = LazyLock::new(|| Arc::new(RwLock::new(vec![])));


impl Header{
    pub async fn get_num() -> usize{
        let mut header_list = blocks::HeadersList.read().await;
        header_list.len()
    }
    pub async fn get_identifier()-> Vec::<u8>{
        (Header::get_num().await as u64).to_le_bytes().to_vec()
    }
    pub fn to_bytes(&self) -> Vec::<u8>{
        let mut buf = Vec::new();
        self.to_protobuf().encode(&mut buf);
        return buf;
    }


    pub fn to_protobuf(&self) -> proto_bufs::Header{
        let saved_bits: Vec<u64> = self.bloom.clone().unwrap().as_slice().to_vec();
        let num_hashes = self.bloom.clone().unwrap().num_hashes();

        let u8_vec: Vec<u8> = saved_bits
            .iter()
            .flat_map(|&x| x.to_ne_bytes()) // Use to_be_bytes() or to_le_bytes() for specific endianness
            .collect();
        let header_protobuf = proto_bufs::Header{
            stake: String::from(""),
            num: self.num as i32,
            key: "".to_string(),
            merkle_root: self.merkle_root.clone(),
            bloom: Some(
                proto_bufs::BloomFilter{
                    filter: u8_vec,
                    num_hashes: num_hashes as i32
                }
            ),
            state_root:self.state_root.clone(),
            previous: self.previous_hash.clone().unwrap(),
            grind_nonce: self.grind_nonce.unwrap_or([0u8; 32]).to_vec()

        };
        return header_protobuf;
    }

    pub fn to_body_protobufs(&self) -> proto_bufs::Block{
        let full_protobuf = proto_bufs::Block{
            body: Some(self.to_protobuf()),
            body_signature: Vec::new(),
            resources: self.resources.clone()
        };
        return full_protobuf;
    }

    pub fn to_body_bytes(&self) -> Vec::<u8>{
        let mut body_encoded = Vec::new();
        self.to_body_protobufs().encode(&mut body_encoded);
        return body_encoded;
    }

    pub fn add_header(pos: u32, merkle_root:Vec::<u8>, filter: Vec::<u64>, hash_nums:u32, resources_total: u32, previous_hash: Option<Vec::<u8>>) -> Header{
        ////////println!("Adding header");
        let new_filter = BloomFilter::from_vec(filter.clone())
            .seed(&12345u128)
            .hashes(hash_nums.clone());
        return Header{
            num:pos,
            merkle_root: merkle_root,
            bloom: Some(new_filter),
            db_conn: None,
            resources_total: resources_total as i32,
            previous_hash: previous_hash,
            resources: Vec::new(),
            merkle_tree: Vec::new(),
            signature: Vec::new(),
            key: "".to_string(),
            state_root: Vec::new(),
            state: None,
            grind_nonce: None
        }
    }






    pub fn check_bloom(&self, grid: String) -> bool{
        self.bloom.clone().unwrap().contains(&grid)
    }


    
   


    pub async fn push_to_list(&self) {
        ////////println!("REQQQ");
        let mut lock = HeadersList.write().await;
        lock.push(self.clone());
        drop(lock)
    }

    pub async fn read_list() {
        let header_list = HeadersList.read().await;
        ////////println!("READDDDDDDDDDDDDD");
        for header in header_list.iter(){
            ////////println!("Found header HEAD");
        }
    }

    pub async fn match_header(hash: Vec::<u8>) -> bool{
        let header_list = HeadersList.read().await;
        for header in header_list.iter(){
            if header.merkle_root == hash{
                return true;
            }
        }
        return false;
    }

    pub async fn check_header(header: &mut blocks::Header, identifier: &Vec::<u8>) -> bool{
        let generated = blocks::Header::create_header(&header.resources, &identifier).await;
        if let Some(comparison_header) = generated{
            println!("Comparison header generated successfully");
            println!("Merkle:\n{}\n{}", hex::encode(&header.merkle_root), hex::encode(&comparison_header.merkle_root));
            println!("State:\n{}\n{}", hex::encode(&header.state_root), hex::encode(&comparison_header.state_root));
            if header.merkle_root == comparison_header.merkle_root{
                header.state = Some(comparison_header.state.unwrap().clone());
                return true;
            }
        }
        panic!("Error: header coulnd't be generated!");
        return false
    }




    pub async fn block_to_head(block: proto_bufs::Block) -> blocks::Header{
        let load_header = block.body.as_ref().unwrap();
        let mut new_header = blocks::Header::add_header(
            load_header.num as u32,
            load_header.merkle_root.clone(),
            load_header.bloom.as_ref().unwrap().filter
                .chunks_exact(8)
                .map(|chunk| {
                    let array: [u8; 8] = chunk.try_into().unwrap();
                    u64::from_ne_bytes(array) // Use from_be_bytes or from_le_bytes for specific endianness
                })
                .collect(),
            load_header.bloom.as_ref().unwrap().num_hashes as u32,
            0,
            Some(Vec::new())
        );
        new_header.state_root = load_header.state_root.clone();
        new_header.resources = block.resources.clone();
        new_header
    }

}