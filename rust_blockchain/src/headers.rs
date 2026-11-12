use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;

use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;
use crate::proto_bufs;
use prost::Message;
use crate::crypt::MerkleNode as MerkleNode;
use crate::mempool::BLOCKHEAD as BLOCKHEAD;
use crate::mempool::DBHead as DBHead;
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::mempool::HEAD_LIST as HEAD_LIST;
use std::env;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;



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
    pub merkle_tree: Vec::<Vec::<MerkleNode>>
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


    pub fn to_bytes(&self) -> Vec::<u8>{
        let saved_bits: Vec<u64> = self.bloom.clone().unwrap().as_slice().to_vec();
        let num_hashes = self.bloom.clone().unwrap().num_hashes();

        let u8_vec: Vec<u8> = saved_bits
            .iter()
            .flat_map(|&x| x.to_ne_bytes()) // Use to_be_bytes() or to_le_bytes() for specific endianness
            .collect();
        let header_protobuf = proto_bufs::Header{
            stake: String::from(""),
            key: "".to_string(),
            num: self.num as i32,
            merkle_root: self.merkle_root.clone(),
            bloom: Some(
                proto_bufs::BloomFilter{
                    filter: u8_vec,
                    num_hashes: num_hashes as i32
                }
            ),
            state_root:self.state_root.clone(),
            previous: self.previous_hash.clone().unwrap()

        };
        let mut buf = Vec::new();
        header_protobuf.encode(&mut buf);
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
            previous: self.previous_hash.clone().unwrap()

        };
        return header_protobuf;
    }

    pub fn add_header(pos: u32, merkle_root:Vec::<u8>, filter: Vec::<u64>, hash_nums:u32, resources_total: u32, previous_hash: Option<Vec::<u8>>) -> Header{
        //println!("Adding header");
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
        }
    }


    pub async fn load_database(mut header: &mut Header) -> &mut Header{
        //println!("DBLOAD");
        //println!("LOADED");
        
        let merkle_encoded = hex::encode(header.merkle_root.clone());
        
        let args: Vec<String> = env::args().collect();

        let db_dir = args[2].clone();
        let file_exists = Path::new(format!("{db_dir}/{merkle_encoded}.db").as_str()).exists();
        if file_exists == false{
            println!("Error: file not in dbs");
            panic!("error message {}", format!("{db_dir}/{merkle_encoded}.db"));
        }
        let new_connection = Connection::open(format!("{db_dir}/{merkle_encoded}.db")).await.expect("ASS");
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
                let mut stmt = conn.prepare(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )?;
                
                // Fix: Explicitly pass empty parameters using the params! macro or an empty slice
                let table_iter = stmt.query_map(params![], |row| row.get::<_, String>(0))?;
                
                let mut tables = Vec::new();
                for table_name in table_iter {
                    tables.push(table_name?);
                }

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
    pub async fn check_Database_revoke(mut header: &mut Header, grid:String) -> bool{
        let mut results:Vec::<u8> = Vec::new();
        if header.db_conn.is_none(){
            Header::load_database(header).await;
        }

        let resource = header.clone().db_conn.clone().unwrap()
        .call(move|conn| {
            let mut resource:Option<Resource> = None;
            let mut proofs: Vec::<Vec::<Vec::<u8>>> = Vec::new();

            let mut stmt = conn.prepare("SELECT grid FROM revocation WHERE grid = $1")?;
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
        let mut results:Vec::<u8> = Vec::new();
        if header.db_conn.is_none(){
            Header::load_database(header).await;
        }
        let header_clone = header.clone();
        let resource = header.clone().db_conn.clone().unwrap()
            .call(move|conn| {
                let mut resource:Option<Resource> = None;
                let mut proofs: Vec::<Vec::<Vec::<u8>>> = Vec::new();

                let mut stmt = conn.prepare("SELECT key, type, value, nonce FROM fields where grid = $1")?;
                let mut fields = stmt
                    .query_map([grid.clone()], |row| {
                        let value_bytes: Vec<u8> = row.get::<_, Option<Vec<u8>>>(2)?.unwrap_or_default();
                        Ok(
                            Field{
                                key: row.get(0)?,
                                r#type: row.get(1)?,
                                value: value_bytes,
                                nonce: row.get(3)?,
                            }
                        )
                    })?
                    .collect::<std::result::Result<Vec<Field>, rusqlite::Error>>()?;

                let mut stmt = conn.prepare("SELECT gid, type, action, nonce FROM global_id WHERE gid = $1")?;
                let mut people = stmt
                    .query_map([grid.clone()], |row| {
                        Ok(
                            Resource{
                                payload:Some(
                                    Payload{
                                        grid: row.get(0)?,
                                        r#type: row.get(1)?,
                                        action: row.get(2)?,
                                        fields: fields.clone(),
                                        nonce: row.get(3)?
                                    }
                                ),
                                status: 0,
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
                    let result = Resource{
                        payload: people[0].payload.clone(),
                        signatures: Vec::new(),
                        proof: vec![MerkleProof{
                            r#type: format!("{}", header_clone.num),
                            hashes: proofs[0].clone(),
                            block: header_clone.num,
                            grid: String::from("")
                        }],
                        status: 0
                    };
                    Ok(Some(result))
                
            })
            .await;
            Some(resource.unwrap()?)
    }

    pub async fn fetch_revoke(mut header: &mut Header, grid:String) -> Option<String>{
        if header.db_conn.is_none(){
            Header::load_database(header).await;
        }
        let header_clone = header.clone();
        let resource = header.clone().db_conn.clone().unwrap()
            .call(move|conn| {
                let mut resource:Option<String> = None;
                let mut stmt = conn.prepare("SELECT parent FROM revocation WHERE grid = $1")?;
                let mut people = stmt
                .query_map([grid.clone()], |row| {
                    Ok(
                        row.get(0)?
                    )
                })?
                .collect::<std::result::Result<Vec<String>, rusqlite::Error>>()?;


                if people.len() == 0{
                    return Ok(None);
                }
                Ok(Some(people[0].clone()))
                
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

    pub async fn match_header(hash: Vec::<u8>) -> bool{
        let header_list = HeadersList.read().await;
        for header in header_list.iter(){
            if header.merkle_root == hash{
                return true;
            }
        }
        return false;
    }

    
}