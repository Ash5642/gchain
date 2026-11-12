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
use rocksdb::{DB, Options, FlushOptions, WriteOptions};
use std::sync::OnceLock;
use tokio::sync::OnceCell;
use async_recursion::async_recursion;


pub struct RandGenerator{
    counter: u64,
    seed_bytes: [u8; 32],
}

#[derive(Clone)]
pub struct Entity{
    pub geid: String,
    pub balance: u64
}

pub struct StateUpdate{
    pub key: Vec::<u8>,
    pub node: proto_bufs::StateNode
}

pub struct StateSession{
    pub db: Arc<rocksdb::DB>,
    pub nodes: Vec::<StateUpdate>,
    pub generator: RandGenerator
}

static STATE_DB: OnceCell<rocksdb::DB> = OnceCell::const_new();
static STATE_STORAGE: OnceCell<Arc<rocksdb::DB>> = OnceCell::const_new();

pub async fn get_db() -> &'static Arc<DB> {
    STATE_STORAGE.get_or_init(|| async {
        // RocksDB opening is a blocking operation, so we offload it to a blocking thread
        tokio::task::spawn_blocking(|| {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            
            println!("Initializing database lock for the first time...");
            let args: Vec<String> = env::args().collect();
            let db_dir = args[2].clone();
            let db_name = format!("{db_dir}/state");
            let db = DB::open(&opts, db_name).unwrap();
            Arc::new(db)
        })
        .await
        .expect("Failed to initialize global database")
    }).await
}


pub async fn initialize_state(){
    let db = get_db().await;
    let root_node: proto_bufs::StateNode;
    match db.get(b"ROOT") {
        Ok(Some(value)) => {
            println!("Key found!");
            println!("Loaded state header: {}", hex::encode(value));

        },
        Ok(None) => {
            println!("Key not found");
            root_node = proto_bufs::StateNode{
                children: vec![vec![0u8; 0]; 16],
                entity: None
            };
            let mut root_encoded = Vec::<u8>::new();
            root_node.encode(&mut root_encoded).unwrap();
            db.put(b"ROOT", root_encoded).unwrap();
        },
        Err(e) => {
            println!("Key not found");
            root_node = proto_bufs::StateNode{
                children: vec![vec![0u8; 0]; 16],
                entity: None
            };
            let mut root_encoded = Vec::<u8>::new();
            root_node.encode(&mut root_encoded).unwrap();
            db.put(b"ROOT", root_encoded).unwrap();
        },
    }
}



#[async_recursion]
pub async fn read_record(geid: &str) -> Option<proto_bufs::StateEntity>{
    let mut geid_hash;
    let mut hasher = Sha256::new();
    hasher.update(&geid.as_bytes());
    geid_hash = hasher.finalize().to_vec();
    let hex_address = hex::encode(geid_hash);
    let mut session = StateSession::start_session().await;

    let db = get_db().await;
    let mut current_node: Option<proto_bufs::StateNode> = Some(session.get_or_create(b"ROOT".to_vec()).await);

    let mut remaining_string = hex_address.to_string();

    while !current_node.is_none(){
        if remaining_string.len() == 0{
            println!("{:#?}", current_node.as_ref().unwrap().entity);
            return current_node.as_ref().unwrap().entity.clone();
        }
        let first_char = remaining_string.chars().next().unwrap();
        let char_index = first_char.to_digit(16).unwrap() as usize;

        let child_key = current_node.as_ref().unwrap().children[char_index].clone();
        if child_key.len() == 0{
            return None
        }
        match db.get(child_key) {
            Ok(Some(value)) => {
                current_node = Some(proto_bufs::StateNode::decode(&(value.clone())[..]).unwrap());
            },
            Ok(None) => {},
            Err(e) => {}
        }
        remaining_string = (&remaining_string[first_char.len_utf8()..]).to_string();

    }
    return None;
}



pub async fn get_root() -> Vec::<u8>{
    let db = get_db().await;
    match db.get(b"ROOT") {
        Ok(Some(value)) => {
            let root = utils::get_hash(value);
            println!("State root: {}", hex::encode(&root));
            return root;

        },
        Ok(None) => {
        },
        Err(e) => {
        },
    }
    return Vec::new();
}


impl StateSession{
    pub async fn start_session() -> StateSession{
        let old_root = get_root().await;
        let new_generator = RandGenerator{
            counter: 0,
            seed_bytes: old_root.try_into().unwrap()
        };
        return StateSession{
            db: get_db().await.clone(),
            nodes: Vec::new(),
            generator: new_generator
        };
    }
    pub fn rand(&mut self) -> Vec::<u8>{
        let mut hasher = Sha256::new();
        hasher.update(self.generator.seed_bytes);
        hasher.update(self.generator.counter.to_le_bytes());
        self.generator.counter = self.generator.counter + 1;
        return hasher.finalize().to_vec()
    }
    pub async fn get_geid(&mut self, geid: &str, value: Option<u64>){
        let mut geid_hash;
        let mut hasher = Sha256::new();
        hasher.update(&geid.as_bytes());
        geid_hash = hasher.finalize().to_vec();
        let hex_address = hex::encode(geid_hash);
        let root_node = self.get_or_create(b"ROOT".to_vec()).await;
        self.get_by_hash(&hex_address[..], root_node, value).await;
    }


    #[async_recursion]
    pub async fn get_by_hash(&mut self, s: &str, mut branch: proto_bufs::StateNode, value: Option<u64>) -> proto_bufs::StateNode{

        if s.is_empty() {
            if branch.entity.is_none(){
                branch.entity = Some(
                    proto_bufs::StateEntity{
                        balance: 0 as u64
                    }
                )
            }
            if !value.is_none(){
                branch.entity = Some(
                    proto_bufs::StateEntity{
                        balance: value.unwrap()
                    }
                );
            }
            self.save_node(branch.clone(), None).await;
            return branch.clone();
        }
        let db = get_db().await;



        let first_char = s.chars().next().unwrap();
        let char_index = first_char.to_digit(16).unwrap() as usize;
        let original_branch = branch.clone();

        let child_key = branch.children[char_index].clone();
        let child_node = self.get_or_create(child_key).await;


        let remaining_string = &s[first_char.len_utf8()..];

        let updated_child_node = self.get_by_hash(remaining_string, child_node.clone(), value).await;

        let mut child_encoded = Vec::<u8>::new();
        updated_child_node.encode(&mut child_encoded).unwrap();
        let child_hash = utils::get_hash(child_encoded);

        branch.children[char_index] = child_hash.clone();

        if s.len() == 64{
            self.save_node(branch.clone(), Some(b"ROOT".to_vec())).await;
        }
        else{
            self.save_node(branch.clone(), None).await;
        }

        return branch;

    }
    #[async_recursion]
    pub async fn get_or_create(&mut self, key: Vec::<u8>) -> proto_bufs::StateNode{
        if key.len() == 0{
            return proto_bufs::StateNode{
                children: vec![vec![0u8; 0]; 16],
                entity: None
            };
        }
        let root_node: proto_bufs::StateNode;
        for node in self.nodes.iter(){
            if node.key == key{
                return node.node.clone();
            }
        }
        match self.db.get(key.clone()) {
            Ok(Some(value)) => {
                let node = proto_bufs::StateNode::decode(&(value.clone())[..]).unwrap();
                return node;
            },
            Ok(None) => {},
            Err(e) => {}
        }
        return proto_bufs::StateNode{
            children: vec![vec![0u8; 0]; 16],
            entity: None
        };
    }
    #[async_recursion]
    pub async fn save_node(&mut self, node: proto_bufs::StateNode, key: Option<Vec::<u8>>){
        let mut node_encoded = Vec::<u8>::new();
        node.encode(&mut node_encoded).unwrap();
        let node_hash = utils::get_hash(node_encoded.clone());
        let db = get_db().await;
        let node_key: Vec::<u8>;
        if !key.is_none(){
            node_key = key.unwrap();
        }
        else{
            node_key = node_hash;
        }
        let mut write_opts = WriteOptions::default();
        for mut saved_node in self.nodes.iter_mut(){
            if saved_node.key == node_key{
                saved_node.node = node.clone();
                return;
            }
        }
        let new_update = StateUpdate{
            key: node_key.clone(),
            node: node.clone()
        };
        self.nodes.push(new_update);
    }

    #[async_recursion]
    pub async fn commit(&self){
        for node in self.nodes.iter(){
            let mut node_encoded = Vec::<u8>::new();
            node.node.encode(&mut node_encoded).unwrap();
            self.db.put(node.key.clone(), node_encoded.clone()).unwrap();
        }
    }

    pub fn get_root(&self) -> Vec::<u8>{
        let last = self.nodes.last();
        if last.is_none(){
            return Vec::new();
        }
        return Vec::new()

    }




}