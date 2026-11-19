use tokio_rusqlite::{params, Connection, Result};
use std::collections::HashSet;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs;
use std::fs;

use tokio::sync::mpsc;
use super::blocks;
use crate::state;
use crate::resources;
use crate::utils;
use crate::entity;
use tokio::io::AsyncWriteExt;
use fastbloom::BloomFilter;
use sha2::{Sha256, Digest};
use prost::Message; 
use crate::vm;


#[derive(Clone)]
#[derive(Debug)]
pub struct MerkleNode{
    pub hash: Vec::<u8>,
    pub leaf:Option<String>,
    pub parent: Option<Vec::<u8>>,
    pub children: Vec::<Box<MerkleNode>>,
}

impl blocks::Header{
    pub fn generate_merkle(commit_list: &Vec::<Resource>, state: &mut state::StateSession) -> Vec::<Vec::<MerkleNode>>{
        let mut nodes: Vec::<Vec::<MerkleNode>> = Vec::new();
        nodes.push(
            Vec::new()
        );
        let item_count = commit_list.len();
        let mut depth:u32 = 2;
        let base: i32 = 2;
        while base.pow(depth+1) < (item_count as i32){
            depth += 1;
        }
        depth+=1;
        //println!("True: {}\nIncreased: {}", item_count, base.pow(depth));
        for item in commit_list.iter(){
            let result = item.to_hash();
            if let Some(inner) = &item.payload {
                let _name_clone = inner.grid.clone();
            }
            let leaf = MerkleNode{
                hash: result.clone(),
                leaf:Some((item.clone().payload).unwrap().grid),
                parent:None,
                children:Vec::new()
            };
            nodes[0].push(leaf);
            let _encoded = hex::encode(&result);
        }
        for extra in item_count..(base.pow(depth) as usize){

            let leaf = MerkleNode{
                hash: state.rand(),
                leaf:None,
                parent:None,
                children:Vec::new()
            };
            nodes[0].push(leaf);
        }
        //println!("Node count: {}", nodes[0].len());
        loop{
            if let Some(top_wrapped) = nodes.last_mut() {
                let top = (*top_wrapped).clone();
                for merkle_hash in &top{
                    let _merkle_hash_encoded = hex::encode(merkle_hash.hash.clone());
                }
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
        nodes
    }
}