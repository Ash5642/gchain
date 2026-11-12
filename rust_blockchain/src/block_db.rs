use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use std::env;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;

use crate::proto_bufs::Field as Field;
use crate::crypt::MerkleNode as MerkleNode;
use crate::mempool::BLOCKHEAD as BLOCKHEAD;
use crate::mempool::DBHead as DBHead;
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::mempool::HEAD_LIST as HEAD_LIST;

use crate::mempool::BLOOMFILTER as BLOOMFILTER;
use crate::headers;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Mutex, OnceLock};
use tokio::task;

static GLOBAL_DB: OnceLock<Connection> = OnceLock::new();


fn db() -> &'static Connection {
    GLOBAL_DB.get().expect("Database connection is not initialized!")
}
#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
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


// --- SEPARATE FUNCTION CALLS ---

async fn setup_database() -> Result<()> {
    if GLOBAL_DB.get().is_none() {
        //////////////////println!("Database is not initialized yet!");
        let conn = Connection::open_in_memory().await?;
        GLOBAL_DB.set(conn).expect("Failed to set global DB");
        
    }else{
        //////////////////println!("Database is initialized!");
        return Ok(())
    }
    let result = db()
        .call(move |conn| {
            conn.execute(
                "CREATE TABLE global_id (
                    gid   STRING PRIMARY KEY,
                    type  STRING NULL DEFAULT 'RESOURCE'
                    action STRING NULL DEFAULT 'ADD'
                )",
                [],
            )?;
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
                    parent BLOB NULL,
                    type  STRING NULL DEFAULT 'ROOT',
                    grid STRING,
                    FOREIGN KEY (grid) REFERENCES global_id(gid)
                )",
                [],
            )?;
            
            Ok(())
        })
    .await?;
    Ok(())
}




pub async fn create_block_db(mut resources:Vec::<Resource>, mut merkle_root: String, merkle_tree: Vec::<Vec::<MerkleNode>>) -> Result<()> {
    let conn = Connection::open_in_memory().await?;
    
    let proofs = conn
        .call(move|conn| {
            conn.execute(
                "CREATE TABLE global_id (
                    gid   STRING PRIMARY KEY,
                    type  STRING NULL DEFAULT 'RESOURCE',
                    action STRING NULL DEFAULT 'ADD'
                )",
                [],
            )?;
            let mut proofs: Vec::<Vec::<Vec::<u8>>> = Vec::new();
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
            let steven = Person {
                id: 1,
                name: "Steven".to_string(),
                data: None,
            };
            for item in resources{
                if let Some(inner) = &item.payload {
                    ////println!("INSERTING: {}", inner.grid);
                    if inner.action == "REVOKE"{
                        //////////////////println!("DBREVOKEIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIi");
                    }
                    let name_clone = inner.grid.clone();
                    conn.execute(
                        "INSERT INTO global_id (gid, type, action) VALUES (?1, ?2, ?3)",
                        params![inner.grid, inner.r#type, inner.action],
                    )?;
                    if let fields = &inner.fields {
                        for field in fields{
                            //////////////////println!("Nested field value: {}", field.key);
                            conn.execute(
                                "INSERT INTO fields (type, grid, key, value, nonce) VALUES (?1, ?2, ?3, ?4, ?5)",
                                params![
                                    field.r#type,
                                    inner.grid,
                                    field.key,
                                    field.value,
                                    field.nonce,
                                    
                                ],
                            )?;
                        }
                    }
                }
                
            }
            for level in merkle_tree{
                for node in level{
                    let encoded = hex::encode(&node.hash);
                    ////////println!("LEAF {:?}", node.leaf);
                    let mut parent_node:Option<Vec::<u8>> = None;
                    if node.parent.is_none() == false{
                        parent_node = Some(node.clone().parent.unwrap());
                        ////////println!("PARENT: {:?}", parent_node.clone());
                    
                        ////////println!("DONE PARENT");
                        conn.execute(
                            "INSERT INTO merkle (value, grid, type, parent) VALUES (?1, ?2, ?3, ?4)",
                            params![
                                node.hash,
                                node.leaf,
                                String::from("NODE"),
                                parent_node.clone()

                                
                            ],
                        )?;
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

            let mut stmt = conn.prepare("SELECT gid, action FROM global_id")?;
            let mut people = stmt
                .query_map([], |row| {
                    Ok(DBResult {
                        resource: DBResource{
                            gid:row.get(0)?,
                            action: row.get(1)?
                        },
                        fields:Vec::new()
                    })
                })?
                .collect::<std::result::Result<Vec<DBResult>, rusqlite::Error>>()?;
            for person in people.iter_mut(){

                let mut stmt = conn.prepare("SELECT value, type, grid FROM merkle where grid = $1")?;
                let mut merkle_nodes = stmt
                    .query_map([&person.resource.gid], |row| {
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


            }
            Ok(proofs)
        })
        .await?;
        

    

    for proof in proofs{
        let proof_chain_clone = proof.clone();
        utils::check_merkle(proof_chain_clone);
    }
    let args: Vec<String> = env::args().collect();
    let db_dir = args[2].clone();
    backup_memory_db_async(&conn, format!("{db_dir}/{merkle_root}.db")).await?;

    Ok(())
    
}







pub async fn check_resource(grid: String, db_name: String) -> Result<(bool)> {
    
    let conn = Connection::open(db_name).await?; 
    let resource = conn
        .call(move|conn| {
            let mut resource:Option<Resource> = None;
            let mut proofs: Vec::<Vec::<Vec::<u8>>> = Vec::new();

            let mut stmt = conn.prepare("SELECT gid, action FROM global_id WHERE gid = $1")?;
            let mut people = stmt
                .query_map([grid], |row| {
                    Ok(DBResult {
                        resource: DBResource{
                            gid:row.get(0)?,
                            action: row.get(1)?
                        },
                        fields:Vec::new()
                    })
                })?
                .collect::<std::result::Result<Vec<DBResult>, rusqlite::Error>>()?;


            if people.len() == 0{
                ////println!("Checked");
                return Ok(false)
            }
            ////println!("UNCHCEKC");
            return Ok(true);
            
        })
        .await?;
        


    Ok(resource)
    
}

pub async fn backup_memory_db_async<P: AsRef<Path>>(conn: &Connection, dest_path: P) -> Result<()> {
    // 1. Convert P into a standard path reference safely
    let path_ref = dest_path.as_ref();
    //////////////////println!("FOUT: {:?}", path_ref);
    // 2. Convert to a string for SQLite execution
    let path_str = path_ref.to_string_lossy().into_owned();

    // 3. Clear file if it exists (VACUUM INTO requires a clean path)
    if path_ref.exists() {
        let _ = tokio::fs::remove_file(path_ref).await;
    }

    conn.call(move |raw_conn| {
        raw_conn.execute("VACUUM main INTO ?1", [&path_str])?;
        Ok(())
    })
    .await?;
    //println!("SAVEEEE");
    Ok(())
}

pub async fn get_grid(item_name:String) -> Result<()> {
    
    setup_database().await?;
    let resource = db()
        .call(move |conn| {
            

            let mut stmt = conn.prepare("SELECT grid, type, nonce FROM fields where grid = $1")?;
            let mut resource = stmt
                .query_map([&item_name], |row| {
                    Ok(
                        Payload {
                            grid: row.get(0)?,
                            r#type: row.get(1)?,
                            nonce: row.get(2)?,
                            fields: Vec::new(),
                            action: String::from("tesy"),
                        }
                    )
                })?
                .collect::<std::result::Result<Vec<Payload>, rusqlite::Error>>()?;
                

            let mut stmt = conn.prepare("SELECT type, key, nonce, value FROM fields where grid = $1")?;
            let people = stmt
                .query_map([&item_name], |row| {
                    Ok(
                        Field {
                            r#type: row.get(0)?,
                            key: row.get(1)?,
                            nonce: row.get(2)?,
                            value: row.get(3)?,
                        }
                    )
                })?
                .collect::<std::result::Result<Vec<Field>, rusqlite::Error>>()?;

            resource[0].fields = people.clone();


            Ok(resource)
        })
        .await?;

        //////////////////println!("In DB:{:?}", resource);
        


    Ok(())
}

pub async fn get_resource2(item_name:String, merkle_root:String) -> Result<()>{

    //////////////////println!("Loading DV");
    let conn = Connection::open(format!("dbs/{merkle_root}.db")).await?; 
    //////////////////println!("Loaded DV");
    // Execute queries using async methods
    let resource = conn
    .call(move |conn| {
        //////////////////println!("FFFFFFFF DV");
        let mut stmt = conn.prepare("SELECT gid, type, action FROM global_id where gid = $1")?;
        //////////////////println!("FFFHFHJBFUUYTgyugggg DV");
        let mut resource = stmt
            .query_map([&item_name], |row| {
                Ok(
                    Payload {
                        grid: row.get(0)?,
                        r#type: row.get(1)?,
                        action: row.get(2)?,
                        nonce: String::from("test").into_bytes(),
                        fields: Vec::new(),
                    }
                )
            })?
            .collect::<std::result::Result<Vec<Payload>, rusqlite::Error>>()?;
            
        
        //////////////////println!("FHBUHFUFHu DV");
        Ok(resource)
    })
    .await?;
    //////////////////println!("DB: {:?}", resource);
    //////////////////println!("Exiting DV");

    Ok(())
}


pub async fn add_block_db(head:BLOCKHEAD) -> Result<()> {
    //////println!("ADDING B:OCK");
    // Open a persistent SQLite file asynchronously
    let args: Vec<String> = env::args().collect();
    let db_dir = args[2].clone();
    let conn = Connection::open("{db_dir}/blocks.db").await?;
    let mut commit_list = HEAD_LIST.read().await;
    let last_block = commit_list.last();
    let last_block_encoded: Vec::<u8>;
    if last_block.is_none(){
        last_block_encoded = Vec::new()
    }
    else{
        //println!("NNNNNNNNNNNNNNNNNNNNNNNNNNNNNN");
        last_block_encoded = last_block.unwrap().clone().to_protobuf();
    }
    //println!("Last element: {:?}", commit_list.last());

    // Execute queries using .call()
    conn.call(move|conn| {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blocks (
                count INT NULL,
                merkle_root BLOB PRIMARY KEY,
                bloom BLOB NULL,
                filter_num INT NULL,
                last BLOB NULL,
                resource_count INT NULL,
                key STRING NULL,
                signature BYTES NULL,
                FOREIGN KEY (last) REFERENCES blocks(merkle_root)
            )",
            [],
        )?;
        
        let u8_vec: Vec<u8> = head.bloom.bits
            .iter()
            .flat_map(|&x| x.to_ne_bytes()) // Use to_be_bytes() or to_le_bytes() for specific endianness
            .collect();
        //println!("Inserting {:?}", commit_list);
        conn.execute(
            "INSERT INTO blocks (merkle_root, bloom, filter_num) VALUES (?1, ?2, ?3)",
            params![head.merkle, u8_vec, head.bloom.num_hashes],
        )?;

        
        Ok(())
    })
    .await?;
    conn.close().await?;
    Ok(())
}


pub async fn load_blocks() -> Result<()> {
    // 1. Open the database file from storage asynchronously
    let args: Vec<String> = env::args().collect();

    println!("Database path: {}", args[2]);
    let db_dir = args[2].clone();
    let conn = Connection::open(format!("{db_dir}/blocks.db")).await?;
    let mut commit_list = HEAD_LIST.write().await;
    //println!("Loading headers");
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
        //println!("LOADFFFDDDD");
        let mut header_pos = 0;
        let mut stmt = conn.prepare("SELECT merkle_root, bloom, filter_num, resource_count, last, height FROM blocks")?;
        let mut header_result = stmt
            .query_map([], |row| {
                header_pos = header_pos + 1;
                Ok(
                    headers::Header::add_header(
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
            .collect::<std::result::Result<Vec<headers::Header>, rusqlite::Error>>()?;
            

        Ok(header_result)
    })
    .await?;
    //println!("DBLOAD SUCCESS");
    for header in header_result{
        //println!("HEHEHEHEHE");
        let header_merkle_encoded = hex::encode(&header.merkle_root);
        println!("{}\t{}\t{}", header.num, header_merkle_encoded, header.resources_total);
        task::spawn(async move {
            header.push_to_list().await;
        }).await;
    }
    task::spawn(async move {
        headers::Header::read_list().await;
    }).await;
    //println!("Continuing");
    conn.close().await?;
    Ok(())
}

pub async fn add_cache_db(grid: String) -> Result<()> {
    //////println!("ADDING B:OCK");
    // Open a persistent SQLite file asynchronously
    //println!("Updating cache list");
    let args: Vec<String> = env::args().collect();
    let db_dir = args[2].clone();
    let conn = Connection::open(format!("{db_dir}/cached.db")).await?;

    conn.call(move|conn| {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cached (
                grid BLOB PRIMARY KEY
            )",
            [],
        )?;

        conn.execute(
            "INSERT INTO cached (grid) VALUES (?1)",
            params![grid],
        )?;

        
        Ok(())
    })
    .await?;
    conn.close().await?;
    Ok(())
}