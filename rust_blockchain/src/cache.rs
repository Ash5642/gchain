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

pub static HostCache: LazyLock<Arc<RwLock<BloomFilter>>> = LazyLock::new(|| Arc::new(RwLock::new(
     BloomFilter::with_false_pos(0.01)
        .seed(&12345u128)
        .expected_items(1000)
)));


pub struct Cache{}

impl Cache{
    pub async fn add_cache_db(grid: String) -> Result<()> {
        //println!("Updating cache list");
        let cache_filter = HostCache.read().await;
        if cache_filter.contains(&grid){
            //println!("Element already in list");
            return Ok(())
        }
        drop(cache_filter);
        let conn = Connection::open("cached.db").await?;
        let grid_clone = grid.clone();
        conn.call(move|conn| {
            conn.execute(
                "INSERT INTO cached (grid) VALUES (?1)",
                params![grid],
            )?;
            Ok(())
        })
        .await?;
        let mut cache_filter = HostCache.write().await;
        cache_filter.insert(&grid_clone);
        drop(cache_filter);
        //println!("Cache updated!");
        conn.close().await?;
        Ok(
            (

            )
        )
    }


    pub async fn load_cache() -> Result<()> {
        //println!("Loading cached");
        let conn = Connection::open("cached.db").await?;
        let mut cache_filter = HostCache.write().await;
        //println!("CACHE DB");
        let header_result = conn.call(move|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cached (
                    grid BLOB PRIMARY KEY
                )",
                [],
            )?;
            let mut stmt = conn.prepare("SELECT grid FROM cached")?;
            let mut header_result = stmt
                .query_map([], |row| {
                    Ok(
                        row.get(0)?
                    )
                })?
                .collect::<std::result::Result<Vec<String>, rusqlite::Error>>()?;
                
            //println!("{:?}", header_result);
            Ok(header_result)
        })
        .await?;
        for header in header_result{
            //println!("HEHEHEHEHE {}", header);
            cache_filter.insert(&header);
        }
        //println!("Continuing");
        conn.close().await?;
        Ok(())
}

}