use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::sync::{Mutex, OnceLock};
use tokio::task;
use crate::mempool::proto_bufs::ByteMessage as ByteMessage;
use crate::mempool::proto_bufs::InternalByteMessage as InternalByteMessage;

use crate::mempool::proto_bufs::NewDocumentPayload as NewDocumentPayload;

use crate::mempool::proto_bufs::MessagePayload as MessagePayload;
use crate::mempool::MEMPOOL as MEMPOOL;
use crate::mempool::DOCUMENT_POOL as DOCUMENT_POOL;

use crate::validations::verify_transaction as verify_transaction;
use crate::validations::verify_blocks as verify_blocks;

use crate::mongo_actions::add_to_ledger as add_to_ledger;
use sha2::{Sha256, Digest};

use rand::prelude::*;
#[derive(Clone, Debug)]
pub struct MerkleNode {
    pub val: String,
    pub is_leaf: bool,
    pub left: Option<Box<MerkleNode>>,
    pub right: Option<Box<MerkleNode>>,
}
impl MerkleNode {
    pub fn new(val: String) -> Self {
        MerkleNode { val, left: None, right: None, is_leaf:false}
    }
}
pub fn build_merkle_tree(mut issuer:String){
    ////////////////println!("MERKED {}", issuer);
    let mut root: MerkleNode = MerkleNode::new("".to_string());
    let mut list = DOCUMENT_POOL.lock().unwrap();
    let mut rng = rand::rng();
    let mut start: &MerkleNode = &root;
    for doc in list.iter(){
        ////////////////println!("{}", doc.grid);
        ////////////////println!("{}", doc.issuer);
        let mut leaf_depth = rng.random_range(0..10);
        ////////////////println!("Integer between 0 and 9: {}", leaf_depth);
        for pos in 0..leaf_depth{
            let mut branch_choice = rng.random_range(0..2);
            if start.left.is_none(){
                start.left = Some(Box::new(MerkleNode::new("".to_string())))
            }
            if start.right.is_none(){
                start.right = Some(Box::new(MerkleNode::new("".to_string())))
            }
            if branch_choice == 0{
                if let Some(left_box) = start.left.take() {
                    start = left_box; 
                }
            }
        }
    }
    return
}


