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
use std::env;
use crate::resources::structures;


impl blocks::Header{
    pub async fn add_block_db(head: &blocks::Header) -> Result<()> {
        let args: Vec<String> = env::args().collect();
        let db_dir = args[2].clone();
        let conn = Connection::open(format!("{db_dir}/blocks.db")).await?;
        let head_clone = head.clone();
        conn.call(move|conn| {
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
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM blocks",
                [],

                |row| row.get(0),
            )?;
            let saved_bits: Vec<u64> = head_clone.bloom.clone().unwrap().as_slice().to_vec();
            let num_hashes = head_clone.bloom.unwrap().num_hashes();
            let u8_vec: Vec<u8> = saved_bits
                .iter()
                .flat_map(|&x| x.to_ne_bytes()) // Use to_be_bytes() or to_le_bytes() for specific endianness
                .collect();
            let _previous_encoded = hex::encode(head_clone.previous_hash.clone().unwrap());
            conn.execute(
                "INSERT INTO blocks (merkle_root, bloom, filter_num, resource_count, last, height, key, signature, count) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![head_clone.merkle_root, u8_vec, num_hashes, head_clone.resources.len(), head_clone.previous_hash, count, "NULL", Vec::new(), head_clone.resources.len()],
            )?;
            
            Ok(())

        })
        .await?;
        
        conn.close().await?;
        head_clone.state.unwrap().commit().await;
        let self_balance = state::read_record(&crate::get_geid().to_string().to_string()).await;
        println!("Self: {:?}", self_balance);

        println!("Header table updated.");
        Ok(())
    }


    pub async fn save_resources(head: &blocks::Header) -> Result<()> {
        let merkle_encoded = hex::encode(&head.merkle_root);
        let args: Vec<String> = env::args().collect();
        let mut commit_global_list = resources::ResourcePool.write().await;
        let to_remove_names: HashSet<String> = head.clone().resources.into_iter().map(|r| r.payload.as_ref().unwrap().grid.clone()).collect();
        commit_global_list.retain(|x| !to_remove_names.contains(&x.payload.as_ref().unwrap().grid));
        let db_dir = args[2].clone();
        let db_name = format!("{db_dir}/{merkle_encoded}.db");
        let conn = Connection::open_in_memory().await?;
        let _base: i32 = 2;
        let head_clone = head.clone();

        conn.call(move|conn| {
            conn.execute(
                "CREATE TABLE global_id (
                    gid   STRING PRIMARY KEY,
                    type  INT NULL DEFAULT 0,
                    action STRING NULL DEFAULT 'ADD',
                    nonce BYTES NULL
                )",
                [],
            )?;
            let _proofs: Vec::<Vec::<Vec::<u8>>> = Vec::new();
            conn.execute(
                "CREATE TABLE fields (
                    type  STRING NULL DEFAULT 'RESOURCE',
                    grid STRING,
                    key STRING,
                    value BLOB,
                    nonce BLOB,
                    field_id INT NULL DEFAULT 0,
                    
                    FOREIGN KEY (grid) REFERENCES global_id(gid)
                )",
                [],
            )?;
            conn.execute(
                "CREATE TABLE merkle (
                    value BLOB PRIMARY KEY,
                    type  STRING NULL DEFAULT 'ROOT',
                    grid STRING,
                    parent BLOB NULL,
                    FOREIGN KEY (grid) REFERENCES global_id(gid)
                )",
                [],
            )?;
            conn.execute(
                "CREATE TABLE revocation (
                    parent STRING,
                    grid STRING PRIMARY KEY,
                    FOREIGN KEY (parent) REFERENCES global_id(gid)
                )",
                [],
            )?;
            for item in head_clone.resources.iter(){
                if let Some(inner) = &item.payload {
                    if inner.r#type == structures::ResourceTypes::Revoke.id{
                    }
                    if inner.r#type == structures::ResourceTypes::Record.id{
                    }
                    let _name_clone = inner.grid.clone();
                    match conn.execute(
                        "INSERT INTO global_id (gid, type, action, nonce) VALUES (?1, ?2, ?3, ?4)",
                        params![inner.grid, inner.r#type, inner.action, inner.nonce],
                    ){
                        Ok(_) => {},
                        Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                    }
                
                    if inner.r#type == structures::ResourceTypes::Revoke.id{
                        let revoked_grid_string = item.get_text_field("grid");
                        let revoked_grids: Vec::<&str> = revoked_grid_string.split(";").collect();
                        for revoked_grid in revoked_grids{
                            match conn.execute(
                                "INSERT INTO revocation (grid, parent) VALUES (?1, ?2)",
                                params![revoked_grid.to_string(), inner.grid],
                            ) {
                                Ok(_) => println!("Success"),
                                Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                            }
                        }
                    }
                    if let fields = &inner.fields {
                        for field in fields{
                            match conn.execute(
                                "INSERT INTO fields (type, grid, key, nonce, value, field_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                params![
                                    field.r#type,
                                    inner.grid,
                                    field.key,
                                    field.nonce,
                                    field.value,
                                    field.id
                                    
                                ],
                            ){
                                Ok(_) => {},
                                Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                            }
                        }
                    }
                }
            }
            for level in head_clone.merkle_tree.iter(){
                for node in level{
                    let _encoded = hex::encode(&node.hash);
                    //////////////println!("LEAF {:?}", node.leaf);
                    let mut parent_node:Option<Vec::<u8>> = None;
                    if !node.parent.is_none(){
                        parent_node = Some(node.clone().parent.unwrap());
                        //////////////println!("PARENT: {:?}", parent_node.clone());
                    
                        //////////////println!("DONE PARENT");
                        match conn.execute(
                            "INSERT INTO merkle (value, grid, type, parent) VALUES (?1, ?2, ?3, ?4)",
                            params![
                                node.hash,
                                node.leaf,
                                String::from("NODE"),
                                parent_node.clone()

                                
                            ],
                        )
                        {
                            Ok(_) => {},
                            Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                        }
                    
                    }else{
                        //////////////println!("DONE ORPHAN");
                        conn.execute(
                            "INSERT INTO merkle (value, grid, type) VALUES (?1, ?2, ?3)",
                            params![
                                node.hash,
                                node.leaf,
                                String::from("NODE")

                                
                            ],
                        )?;
                    }

                }
            }


            Ok(())
        })
        .await?;

        println!("SUCCESS!!!");
        conn.call(move |raw_conn| {
            raw_conn.execute("VACUUM main INTO ?1", [&db_name])?;
            Ok(())
        })
        .await?;
        println!("Saved to disk!");
        conn.close().await?;
        Ok(())
    }


    pub async fn save(&self){
        blocks::Header::save_resources(self).await;
        blocks::Header::add_block_db(self).await;
    }
}