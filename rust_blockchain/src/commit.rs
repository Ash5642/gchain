use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::sync::{Mutex, OnceLock};
use tokio::task;
use sha2::{Digest, Sha256};

use crate::mempool::proto_bufs::ByteMessage as ByteMessage;

use crate::mempool::MEMPOOL as MEMPOOL;
pub mod proto_bufs {
    include!(concat!(env!("OUT_DIR"), "/proto_bufs.rs"));
}


pub fn commit_transaction(mut message:ByteMessage) -> bool{
    // Acquire the lock (blocks current async task until acquired)
    // Note: use 'tokio::sync::Mutex' instead if your write operations are long-running
    let mut hasher = Sha256::new();
    ////////////////println!("Validating message of type: {}!", &message.message_type);
    hasher.update(b"message.message_type");
    let result = hasher.finalize();
    let included_sign = message.signature.clone();
    message.signature = hex::encode(&result);
    // Convert the result to a hexadecimal string
    ////////////////println!("Calculated SHA-256 Hash: {:x}", result);
    ////////////////println!("Included SHA-256 Hash: {}", included_sign.clone());
    let mut buf = Vec::new();
    message.encode(&mut buf).unwrap();
    ////////////////println!("Serialized data size: {} bytes", buf.len());
    let hex_string = hex::encode(&buf);
    ////////////////println!("Corrected serialized (message id: {}): {}", message.message_type, hex_string);
    true
    // Lock is automatically released here

}
