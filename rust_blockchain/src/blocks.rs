use tokio_rusqlite::{params, Connection, Result};
use std::collections::HashSet;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs;
use std::fs;
use crate::cash;
use crate::crypt::MerkleNode as MerkleNode;
use tokio::sync::mpsc;
use crate::headers;
use crate::state;
use crate::resources;
use crate::utils;
use crate::entity;
use tokio::io::AsyncWriteExt;
use fastbloom::BloomFilter;
use sha2::{Sha256, Digest};
use prost::Message; 
use crate::resources::ChainResource;
use crate::vm;
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
use std::env;
impl headers::Header{

    pub fn generate_merkle(commit_list: Vec::<Resource>) -> Vec::<Vec::<MerkleNode>>{
        let mut nodes: Vec::<Vec::<MerkleNode>> = Vec::new();
        nodes.push(
            Vec::new()
        );
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
            ////////////////println!("hash {}: {}", i, encoded);   
        }
        //println!("Merkle proofs generated!");
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

    pub fn generate_bloom(commit_list: Vec::<Resource>) -> BloomFilter{
        let _my_seed = 12345u128; 
        let _base: i32 = 2;
        let mut filter = BloomFilter::with_false_pos(0.01)
            .seed(&12345u128)
            .expected_items(commit_list.len()); // Correctly constructs the BloomFilter<DefaultHasher>
        for resource_cloned in commit_list.iter(){
            let mut revoke_str: String;
            if resource_cloned.payload.as_ref().unwrap().r#type == "REVOKE"{
                let revoked_grid_string = resource_cloned.get_text_field("grid");
                println!("REV {}", revoked_grid_string);
                let revoked_grids: Vec::<&str> = revoked_grid_string.split(";").collect();
                for revoked_grid in revoked_grids{
                    println!("REVOKING SUB {}", revoked_grid);
                    revoke_str = format!("-{}", revoked_grid);
                    filter.insert(&revoke_str);
                }
            }
            revoke_str = format!("+{}", resource_cloned.payload.as_ref().unwrap().grid);
            filter.insert(&revoke_str);
            
        }
        filter
    }


    pub async fn add_block_db(head:headers::Header) -> Result<()> {
        let args: Vec<String> = env::args().collect();
        println!("SAVINGGBGJHUGHUGHGU HEAD to database!");
        let db_dir = args[2].clone();
        let conn = Connection::open(format!("{db_dir}/blocks.db")).await?;
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
            let saved_bits: Vec<u64> = head.bloom.clone().unwrap().as_slice().to_vec();
            let num_hashes = head.bloom.unwrap().num_hashes();
            let u8_vec: Vec<u8> = saved_bits
                .iter()
                .flat_map(|&x| x.to_ne_bytes()) // Use to_be_bytes() or to_le_bytes() for specific endianness
                .collect();
            let _previous_encoded = hex::encode(head.previous_hash.clone().unwrap());
            conn.execute(
                "INSERT INTO blocks (merkle_root, bloom, filter_num, resource_count, last, height, key, signature, count) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![head.merkle_root, u8_vec, num_hashes, head.resources_total, head.previous_hash, count, "NULL", Vec::new(), head.resources.len()],
            )?;
            
            Ok(())

        })
        .await?;
        
        conn.close().await?;
        println!("Header table updated.");
        Ok(())
    }

    pub async fn save_resources2(head:headers::Header, commit_list: Vec::<Resource>, depth: u32, merkle_tree: Vec::<Vec::<MerkleNode>>) -> Result<()> {
        let merkle_encoded = hex::encode(&head.merkle_root);
        let args: Vec<String> = env::args().collect();

        println!("Database path: {}", args[2]);
        let db_dir = args[2].clone();
        let db_name = format!("{db_dir}/{merkle_encoded}.db");
        let conn = Connection::open_in_memory().await?;
        let base: i32 = 2;
        println!("DB_PROCESSING");

        conn.call(move|conn| {
            //println!("DIIHUHIGDUYGDUYGDUGDUGDGDJDD");
            conn.execute(
                "CREATE TABLE global_id (
                    gid   STRING PRIMARY KEY,
                    type  STRING NULL DEFAULT 'RESOURCE',
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
            for i in 0..(base as usize).pow(depth){
                let item = commit_list[i].clone();
                if let Some(inner) = &item.payload {
                    if inner.r#type == "REVOKE"{
                        println!("REVOKING: {}", inner.grid);
                    }
                    
                    let _name_clone = inner.grid.clone();
                    match conn.execute(
                        "INSERT INTO global_id (gid, type, action, nonce) VALUES (?1, ?2, ?3, ?4)",
                        params![inner.grid, inner.r#type, inner.action, inner.nonce],
                    ){
                        Ok(_) => {},
                        Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                    }
                
                    if inner.r#type == "REVOKE"{
                        let revoked_grid_string = item.get_text_field("grid");
                        let revoked_grids: Vec::<&str> = revoked_grid_string.split(";").collect();
                        println!("RA");
                        for revoked_grid in revoked_grids{
                            
                            match conn.execute(
                                "INSERT INTO revocation (grid, parent) VALUES (?1, ?2)",
                                params![revoked_grid.to_string(), inner.grid],
                            ) {
                                Ok(_) => println!("Success"),
                                Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                            }
                        }
                        println!("RB");
                    }
                    if let fields = &inner.fields {
                        for field in fields{
                            //////////////////println!("Nested field value: {}", field.key);
                            match conn.execute(
                                "INSERT INTO fields (type, grid, key, nonce, value) VALUES (?1, ?2, ?3, ?4, ?5)",
                                params![
                                    field.r#type,
                                    inner.grid,
                                    field.key,
                                    field.nonce,
                                    field.value
                                    
                                ],
                            ){
                                Ok(_) => {},
                                Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                            }
                        }
                    }
                }
            }
            for level in merkle_tree.iter(){
                for node in level{
                    let _encoded = hex::encode(&node.hash);
                    ////////println!("LEAF {:?}", node.leaf);
                    let mut parent_node:Option<Vec::<u8>> = None;
                    if !node.parent.is_none(){
                        parent_node = Some(node.clone().parent.unwrap());
                        ////////println!("PARENT: {:?}", parent_node.clone());
                    
                        ////////println!("DONE PARENT");
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
                        ////////println!("DONE ORPHAN");
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
        println!("Saved: {} database!", db_name.clone());
        //println!("SUCCESS!!!");
        conn.call(move |raw_conn| {
            raw_conn.execute("VACUUM main INTO ?1", [&db_name])?;
            Ok(())
        })
        .await?;
        println!("Saved to disk!");
        conn.close().await?;
        Ok(())
    }

    pub async fn save_resources(head:headers::Header) -> Result<()> {
        let merkle_encoded = hex::encode(&head.merkle_root);
        let args: Vec<String> = env::args().collect();
        let mut commit_global_list = resources::ResourcePool.write().await;
        println!("Before save: {}", commit_global_list.len());
        let to_remove_names: HashSet<String> = head.clone().resources.into_iter().map(|r| r.payload.as_ref().unwrap().grid.clone()).collect();
        commit_global_list.retain(|x| !to_remove_names.contains(&x.payload.as_ref().unwrap().grid));
        println!("After save: {}", commit_global_list.len());
        println!("Database path: {}", args[2]);
        let db_dir = args[2].clone();
        let db_name = format!("{db_dir}/{merkle_encoded}.db");
        let conn = Connection::open_in_memory().await?;
        let _base: i32 = 2;
        println!("DB_PROCESSING");
        let head_clone = head.clone();

        conn.call(move|conn| {
            //println!("DIIHUHIGDUYGDUYGDUGDUGDGDJDD");
            conn.execute(
                "CREATE TABLE global_id (
                    gid   STRING PRIMARY KEY,
                    type  STRING NULL DEFAULT 'RESOURCE',
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
            for item in head.resources.iter(){
                if let Some(inner) = &item.payload {
                    if inner.r#type == "REVOKE"{
                        println!("REVOKING: {}", inner.grid);
                    }

                    if inner.r#type == "RECORD"{
                        
                    }
                    
                    let _name_clone = inner.grid.clone();
                    match conn.execute(
                        "INSERT INTO global_id (gid, type, action, nonce) VALUES (?1, ?2, ?3, ?4)",
                        params![inner.grid, inner.r#type, inner.action, inner.nonce],
                    ){
                        Ok(_) => {},
                        Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                    }
                
                    if inner.r#type == "REVOKE"{
                        let revoked_grid_string = item.get_text_field("grid");
                        let revoked_grids: Vec::<&str> = revoked_grid_string.split(";").collect();
                        println!("RA");
                        for revoked_grid in revoked_grids{
                            println!("INNER REVOKE: {}", revoked_grid);
                            match conn.execute(
                                "INSERT INTO revocation (grid, parent) VALUES (?1, ?2)",
                                params![revoked_grid.to_string(), inner.grid],
                            ) {
                                Ok(_) => println!("Success"),
                                Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                            }
                        }
                        println!("RB");
                    }
                    if let fields = &inner.fields {
                        for field in fields{
                            //////////////////println!("Nested field value: {}", field.key);
                            match conn.execute(
                                "INSERT INTO fields (type, grid, key, nonce, value) VALUES (?1, ?2, ?3, ?4, ?5)",
                                params![
                                    field.r#type,
                                    inner.grid,
                                    field.key,
                                    field.nonce,
                                    field.value
                                    
                                ],
                            ){
                                Ok(_) => {},
                                Err(err) => println!("Rusqlite error: {}", err), // Prints the error message
                            }
                        }
                    }
                }
            }
            for level in head.merkle_tree.iter(){
                for node in level{
                    let _encoded = hex::encode(&node.hash);
                    ////////println!("LEAF {:?}", node.leaf);
                    let mut parent_node:Option<Vec::<u8>> = None;
                    if !node.parent.is_none(){
                        parent_node = Some(node.clone().parent.unwrap());
                        ////////println!("PARENT: {:?}", parent_node.clone());
                    
                        ////////println!("DONE PARENT");
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
                        ////////println!("DONE ORPHAN");
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
        println!("Saved: {} database!", db_name.clone());
        let mut state_session = state::StateSession::start_session().await;
        for resource in head_clone.resources.iter(){
            let entity = entity::Entity::get_entity(resource.clone().payload.unwrap().grid);
            let entity_geid = entity.get_geid();
            println!("Updting balance for: {}", entity_geid);
            state_session.get_geid(entity_geid.as_str(), Some(160 as u64)).await;
        }
        state_session.commit().await;
        //println!("SUCCESS!!!");
        conn.call(move |raw_conn| {
            raw_conn.execute("VACUUM main INTO ?1", [&db_name])?;
            Ok(())
        })
        .await?;
        println!("Saved to disk!");
        conn.close().await?;
        Ok(())
    }


    pub async fn generate_header(identifier: Vec::<u8>) -> headers::Header{
        let _root = MerkleNode{
            hash: Vec::new(),
            leaf:None,
            parent:None,
            children:Vec::new(),
        };
        
        let mut commit_global_list = resources::ResourcePool.write().await;
        let mut header_list = headers::HeadersList.write().await;
        let previous_hash: Vec::<u8>;
        if header_list.is_empty(){
            //println!("Genesis block");
            previous_hash = Vec::new();
        }
        else{
            let last_header = header_list.last();
            //println!("New block");
            previous_hash = utils::get_hash(last_header.unwrap().to_bytes());
        }
        if commit_global_list.len() < 3{
            println!("Not enough resources to confirm!");
        }
        let _last_header = header_list.last();
        let mut depth:u32 = 2;
        let base: i32 = 2;
        while base.pow(depth+1) <commit_global_list.len().try_into().unwrap(){
            depth += 1;
        }
        let mut commit_list: Vec::<Resource> = Vec::new();
        let _identifier_encoded = hex::encode(identifier);
        let _geid = crate::get_geid().to_string();
        commit_list.push(
            cash::Cash::faucet(base.pow(depth)).await
        );
        let mut new_session = state::StateSession::start_session().await;
        for i in 1..(base as usize).pow(depth){
            println!("Adding {}", commit_global_list[i].payload.as_ref().unwrap().grid);
            commit_list.push(commit_global_list[i].clone());
        }
        let mut nodes = headers::Header::generate_merkle(commit_list.to_vec());
        let bloom_filter = headers::Header::generate_bloom(commit_list.to_vec());
        let _root_encoded = hex::encode(&nodes.last_mut().unwrap()[0].hash);
        
        
        
        let _root_encoded = hex::encode(&nodes.last_mut().unwrap()[0].hash);

        let mut new_header = headers::Header{
            num: 0,
            merkle_root: nodes.last_mut().unwrap()[0].hash.clone(),
            bloom: Some(bloom_filter),
            db_conn:None,
            resources_total: (base as usize).pow(depth) as i32, 
            previous_hash: Some(previous_hash),
            resources: Vec::new(),
            merkle_tree: nodes.clone(),
            key: "".to_string(),
            signature: Vec::new(),
            state_root: headers::Header::state_generate(&(commit_list[0..(base as usize).pow(depth)].to_vec())).await
        };
        new_header.resources = commit_list[0..(base as usize).pow(depth)].to_vec();
        
        header_list.push(new_header.clone());
        new_header
        
    }

    pub async fn state_generate(resources: &Vec::<Resource>) -> Vec::<u8>{
        let mut state_session = state::StateSession::start_session().await;
        for resource in resources.iter(){
            let entity = entity::Entity::get_entity(resource.clone().payload.unwrap().grid);
            let entity_geid = entity.get_geid();
            println!("Updting balance for: {}", entity_geid);
            state_session.get_geid(entity_geid.as_str(), Some(160 as u64)).await;
        }
        return state_session.get_root();
    }

    pub async fn generate_send_header(tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        let new_header = headers::Header::generate_header(Vec::<u8>::new()).await;
        println!("New header: {:?}", new_header);
        let merkle_encoded = hex::encode(new_header.merkle_root.clone());
        let args: Vec<String> = env::args().collect();
        let db_dir = args[2].clone();
        let db_name = format!("{db_dir}/{merkle_encoded}.db");
        let bytes: Vec<u8> = fs::read(db_name).unwrap();
        let new_block = proto_bufs::NewBlock{
            header: Some(new_header.to_protobuf()),
            resources: bytes
        };
        let mut new_block_encoded = Vec::<u8>::new();
        new_block.encode(&mut new_block_encoded).unwrap();
        let e_resp = Some(
            proto_bufs::ChainMessage{
                r#type: String::from("BLOCK"),
                message: new_block_encoded
            }
        );

        let new_tx_msg = tx_msg.clone();

        if new_tx_msg.send(e_resp.clone().unwrap()).await.is_err() {
            println!("Consumer loop shut down. Exiting producer loop.");
        }
    }

    pub async fn check_header(header: proto_bufs::Block) -> bool{
        let mut nodes = headers::Header::generate_merkle(header.resources.clone());
        let generated_merkle = nodes.last_mut().unwrap()[0].hash.clone();
        let generated_state = headers::Header::state_generate(&header.resources).await;
        if generated_state == header.body.as_ref().unwrap().state_root{
            println!("State updates valid!");
        }
        else{
            println!("SNDJDHJDFHDF");
        }
        generated_merkle == header.body.as_ref().unwrap().merkle_root
    }

    pub async fn block_to_head(block: proto_bufs::Block) -> headers::Header{
        let load_header = block.body.as_ref().unwrap();
        let mut new_header = headers::Header::add_header(
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
        new_header.resources = block.resources.clone();
        new_header
    }

}