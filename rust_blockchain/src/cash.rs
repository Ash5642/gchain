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
use rand::RngExt;
use rand::Rng;
use crate::state;
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
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
#[derive(Debug)] 
#[derive(Clone)]
pub struct Cash{
    grid: String,
    value: u32,
    header: u32,
    confirmed: bool,
    spent: bool
}


pub static CashList: LazyLock<Arc<RwLock<Vec<Cash>>>> = LazyLock::new(|| Arc::new(RwLock::new(vec![])));

impl Cash{


    
    

    pub async fn faucet(value: i32) -> Resource{
        let mut nonce = vec![0u8; 8];
        rand::rng().fill_bytes(&mut nonce);
        let identifier_encoded = hex::encode(nonce);
        let geid = crate::get_geid().to_string();
        let cash = Resource{
            payload:Some(
                proto_bufs::Payload{
                    grid: format!("{geid}[CASH]={identifier_encoded}"),
                    action: "ADD".to_string(),
                    fields: vec![
                        proto_bufs::Field{
                            key: "value".to_string(),
                            r#type: "value".to_string(),
                            nonce: Vec::new(),
                            value: (value as i32).to_le_bytes().to_vec()
                        }
                    ],
                    r#type: "CASH".to_string(),
                    nonce: Vec::new()
                }
            ),
            proof: Vec::new(),
            signatures: Vec::new(),
            status: 0
        };
        
        return cash

    }


    pub async fn create_transaction(sender: &str, reciever: &str, amt: u64, identifier: &str) -> Resource{
        println!("Creating transaction from {} to {} of {}", sender, reciever, amt);

        let transaction = Resource{
            payload: Some(
                Payload{
                    grid:format!("{reciever}[CASH]={identifier}"),
                    r#type: "CASH".to_string(),
                    action: "CREATE".to_string(),
                    fields: vec![
                        Field{
                            key: "FROM".to_string(),
                            r#type: "gid".to_string(),
                            value: sender.bytes().collect(),
                            nonce: Vec::new()
                        },
                        Field{
                            key: "TO".to_string(),
                            r#type: "gid".to_string(),
                            value: reciever.bytes().collect(),
                            nonce: Vec::new()
                        }
                    ],
                    nonce: Vec::new()
                }
            ),
            status: 0,
            proof: Vec::new(),
            signatures: Vec::new()
        };

        return transaction;

    }
        
}




