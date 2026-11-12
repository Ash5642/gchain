use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::sync::{Mutex, OnceLock};
use tokio::task;
use crate::mempool::MEMPOOL as MEMPOOL;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_POOL as MESSAGE_POOL;
use crate::mempool;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::ChainMessage as ChainMessage;

use crate::block_db;

pub async fn add_to_msgpool(mut item:ChainMessage) {
    let mut list;

    list = MESSAGE_POOL.lock().unwrap();

    list.push(item);

}

pub async fn add_to_processpool(mut item:ChainMessage) {
    let mut list;
    list = mempool::PROCESS_POOL.lock().unwrap();
    ////////////////println!("Created process");
    list.push(item);

}

pub async fn add_to_msgqueue(mut item:ChainMessage) {
    let mut list;
    list = mempool::MESSAGE_QUEUE.write().await;
    ////////////////println!("Created process");
    list.push(item);
    drop(list);

}

pub async fn clear_msgqueue() {
    let mut list;
    list = mempool::MESSAGE_QUEUE.write().await;
    list.clear();
    drop(list);
    ////////////////println!("CLEARING");

}