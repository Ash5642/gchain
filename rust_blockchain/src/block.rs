use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;

use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;

use crate::crypt::MerkleNode as MerkleNode;
use crate::mempool::BLOCKHEAD as BLOCKHEAD;
use crate::mempool::DBHead as DBHead;
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::mempool::HEAD_LIST as HEAD_LIST;


use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Header{
    pub pos:u32,
    pub merkle_root:Vec::<u8>,
    pub bloom:Option<BloomFilter>,
    pub db_conn: Option<Connection>
}
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
pub static HeaderList: LazyLock<RwLock<Vec<Header>>> = LazyLock::new(|| {
    RwLock::new(Vec::new()) // Initializes an empty vector
}); 

pub static HeadersList: LazyLock<Arc<RwLock<Vec<Header>>>> = LazyLock::new(|| Arc::new(RwLock::new(vec![])));



impl Header{

    pub fn add_header(pos: u32, merkle_root:Vec::<u8>, filter: Vec::<u64>, hash_nums:u32) -> Header{
        //println!("Adding header");
        let new_filter = BloomFilter::from_vec(filter.clone())
            .seed(&12345u128)
            .hashes(hash_nums.clone());
        return Header{
            pos:pos,
            merkle_root: merkle_root,
            bloom: Some(new_filter),
            db_conn: None,
        }
    }


    pub async fn load_database(mut header: &mut Header) -> &mut Header{
        //println!("DBLOAD");
        //println!("LOADED");
        
        let merkle_encoded = hex::encode(header.merkle_root.clone());
        let new_connection = Connection::open(format!("{merkle_encoded}.db")).await.expect("error");
        header.db_conn = Some(new_connection);
        //println!("Database loaded");
        return header
    }

    pub async fn load_database_header(mut target: &Header){
        //println!("DBLOAD");

    }


    pub async fn check_headers(){
        //println!("Checking...");
        let header_list = HeadersList.read().await;
        let header_list_clone = header_list.clone();
        drop(header_list);
        for header in header_list_clone.iter(){
            

        }
    }

    pub fn check_bloom(&self, grid: String) -> bool{
        self.bloom.clone().unwrap().contains(&grid)
    }


    pub async fn check_Database(mut header: &mut Header, grid:String) -> bool{
        //println!("STARTING DB CHECK");
        let mut results:Vec::<u8> = Vec::new();
        if header.db_conn.is_none(){
            Header::load_database(header).await;
        }
        else{
            //println!("Using loaded database");
        }
        let resource = header.clone().db_conn.clone().unwrap()
            .call(move|conn| {
                let mut resource:Option<Resource> = None;
                let mut proofs: Vec::<Vec::<Vec::<u8>>> = Vec::new();

                let mut stmt = conn.prepare("SELECT gid FROM global_id WHERE gid = $1")?;
                let mut people = stmt
                    .query_map([grid], |row| {
                        Ok(0)
                    })?
                    .collect::<std::result::Result<Vec<u8>, rusqlite::Error>>()?;


                if people.len() == 0{
                    return Ok(false)
                }
                return Ok(true)
                
            })
            .await;
            resource.unwrap()
    }
    pub async fn fetch_database(mut header: &mut Header, grid:String) -> Option<Resource>{
        //println!("STARTING DB CHECK");
        let mut results:Vec::<u8> = Vec::new();
        if header.db_conn.is_none(){
            //println!("Loading database");
            Header::load_database(header).await;
        }
        else{
            //println!("Using loaded database");
        }
        let resource = header.clone().db_conn.clone().unwrap()
            .call(move|conn| {
                let mut resource:Option<Resource> = None;
                let mut proofs: Vec::<Vec::<Vec::<u8>>> = Vec::new();

                let mut stmt = conn.prepare("SELECT gid, type, action FROM global_id WHERE gid = $1")?;
                let mut people = stmt
                    .query_map([grid.clone()], |row| {
                        Ok(
                            Resource{
                                payload:Some(
                                    Payload{
                                        grid: row.get(0)?,
                                        r#type: row.get(1)?,
                                        action: row.get(2)?,
                                        fields: Vec::new(),
                                        nonce: Vec::new()
                                    }
                                ),
                                proof: Vec::new(),
                                signatures:Vec::new()
                            }
                        )
                    })?
                    .collect::<std::result::Result<Vec<Resource>, rusqlite::Error>>()?;
                    if people.len() == 0{
                        return Ok(None);
                    }
                    let mut stmt = conn.prepare("SELECT value, type, grid FROM merkle where grid = $1")?;
                    let mut merkle_nodes = stmt
                        .query_map([grid], |row| {
                            Ok(
                                MerkleNode{
                                    hash: row.get(0)?,
                                    leaf:row.get(2)?,
                                    parent:None,
                                    children:Vec::new(),
                                }
                            )
                        })?;
                    
                    let mut current_node:Option<Vec::<u8>> = None;
                    let mut proof_chain:Vec::<Vec::<u8>> = Vec::new();
                    for merkle_node in merkle_nodes{
                        current_node = Some(merkle_node.unwrap().hash);
                        proof_chain.push(current_node.clone().unwrap());
                    }
                    while true{
                        
                        let mut stmt = conn.prepare("SELECT value, type, grid, parent FROM merkle where value = $1")?;
                        let mut merkle_nodes = stmt
                        .query_map([&current_node.clone().unwrap()], |row| {
                            Ok(
                                MerkleNode{
                                    hash: row.get(0)?,
                                    leaf:row.get(2)?,
                                    parent:row.get(3)?,
                                    children:Vec::new(),
                                }
                            )
                        })?
                        .collect::<std::result::Result<Vec<MerkleNode>, rusqlite::Error>>()?;
                        let old_node = current_node.clone();
                        for merkle_node in merkle_nodes{
                            current_node = merkle_node.parent;
                        }
                        if current_node.is_none(){
                            proof_chain.push(old_node.unwrap());
                            break;
                        }
                        let mut stmt = conn.prepare("SELECT value, type, grid, parent FROM merkle where parent = $1")?;
                        
                        let mut sibling_nodes = stmt
                        .query_map([&current_node.clone().unwrap()], |row| {
                            Ok(
                                MerkleNode{
                                    hash: row.get(0)?,
                                    leaf:row.get(2)?,
                                    parent:row.get(3)?,
                                    children:Vec::new(),
                                }
                            )
                        })?
                        .collect::<std::result::Result<Vec<MerkleNode>, rusqlite::Error>>()?;
                            
                        for sibling in sibling_nodes{
                            proof_chain.push(sibling.clone().hash);
                        }

                    }
                    proofs.push(proof_chain.clone());
                    //println!("PROOF:");
                    for proof in &proof_chain{
                        let encode = hex::encode(&proof);
                        //println!("{}", encode);
                    }
                    let result = Resource{
                        payload: people[0].payload.clone(),
                        signatures: Vec::new(),
                        proof: vec![MerkleProof{
                            hashes: proofs[0].clone()
                        }],
                    };
                    Ok(Some(result))
                
            })
            .await;
            Some(resource.unwrap()?)
    }


    pub async fn push_to_list(&self) {
        //println!("REQQQ");
        let mut lock = HeadersList.write().await;
        lock.push(self.clone());
        drop(lock)
    }

    pub async fn read_list() {
        let header_list = HeadersList.read().await;
        //println!("READDDDDDDDDDDDDD");
        for header in header_list.iter(){
            //println!("Found header HEAD");
        }
    }
}