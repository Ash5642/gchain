

use tokio_rusqlite::{params, Connection, Result};
use std::collections::HashSet;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs;
use std::fs;

use tokio::sync::mpsc;
use crate::state;
use crate::resources;
use crate::utils;
use crate::entity;
use super::blocks::Header as Header;
use super::blocks::merkle::MerkleNode as MerkleNode;

use fastbloom::BloomFilter;
use sha2::{Sha256, Digest};
use prost::Message; 
use crate::vm;
use super::blocks;



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


impl blocks::Header{
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
                                id: 1,
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
                                        permissions: vec![],
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
                    while false{
                        
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
                            r#type: format!("+"),
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
    pub async fn check_Database(mut header: &mut Header, grid:String) -> bool{
        let mut results:Vec::<u8> = Vec::new();
        if header.db_conn.is_none(){
            Header::load_database(header).await;
        }
        else{
            ////////println!("Using loaded database");
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
    pub async fn load_database(mut header: &mut Header) -> &mut Header{   

        let merkle_encoded = hex::encode(header.merkle_root.clone());
        
        let args: Vec<String> = env::args().collect();

        let db_dir = args[2].clone();
        let file_exists = Path::new(format!("{db_dir}/{merkle_encoded}.db").as_str()).exists();
        if file_exists == false{
            //////println!("Error: file not in dbs");
            panic!("error message {}", format!("{db_dir}/{merkle_encoded}.db"));
        }
        let new_connection = Connection::open(format!("{db_dir}/{merkle_encoded}.db")).await.expect("error");
        header.db_conn = Some(new_connection);
        ////////println!("Database loaded");
        return header
    }

    pub async fn load_blocks() -> Result<()> {
    // 1. Open the database file from storage asynchronously
        let args: Vec<String> = env::args().collect();

        ////println!("Database path: {}", args[2]);
        let db_dir = args[2].clone();
        let conn = Connection::open(format!("{db_dir}/blocks.db")).await?;
        //////println!("Loading headers");
        let header_result = conn.call(move|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS blocks (
                    count INT NULL,
                    hash BLOB PRIMARY KEY,
                    height INT NULL,
                    merkle_root BLOB,
                    bloom BLOB NULL,
                    filter_num INT NULL,
                    last BLOB NULL,
                    resource_count INT NULL,
                    key STRING NULL,
                    signature BYTES NULL,
                    FOREIGN KEY (last) REFERENCES blocks(hash)
                )",
                [],
            )?;
            //////println!("LOADFFFDDDD");
            let mut header_pos = 0;
            let mut stmt = conn.prepare("SELECT merkle_root, bloom, filter_num, resource_count, last, height FROM blocks")?;
            let mut header_result = stmt
                .query_map([], |row| {
                    header_pos = header_pos + 1;
                    Ok(
                        blocks::Header::add_header(
                            row.get(5)?,
                            row.get(0)?,
                            row.get::<_, Vec<u8>>(1)?
                                .chunks_exact(8)
                                .map(|chunk| {
                                    let array: [u8; 8] = chunk.try_into().unwrap();
                                    u64::from_ne_bytes(array) // Use from_be_bytes or from_le_bytes for specific endianness
                                })
                                .collect(),
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?
                        )
                    )
                })?
                .collect::<std::result::Result<Vec<blocks::Header>, rusqlite::Error>>()?;
                

            Ok(header_result)
        })
        .await?;
        //////println!("DBLOAD SUCCESS");
        for header in header_result{
            //////println!("HEHEHEHEHE");
            let header_merkle_encoded = hex::encode(&header.merkle_root);
            println!("{}\t{}\t{}", header.num, header_merkle_encoded, header.resources_total);
            task::spawn(async move {
                header.push_to_list().await;
            }).await;
        }
        task::spawn(async move {
            blocks::Header::read_list().await;
        }).await;
        //////println!("Continuing");
        conn.close().await?;
        Ok(())
    }
}