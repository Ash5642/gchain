use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
use std::fs;
use std::env;
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Mutex, OnceLock};
use tokio::task;
use rand::prelude;
use rand::Rng;
use std::error::Error;
use std::time::Duration;
use crate::mempool::MEMPOOL as MEMPOOL;
use crate::mempool::MESSAGE_POOL as MESSAGE_POOL;
use crate::mempool::PROCESS_POOL as PROCESS_POOL;
use crate::mempool::PROCESS_QUEUE as PROCESS_QUEUE;
use crate::mempool::PROCESS as PROCESS;

use crate::consensus;
use crate::mempool;
use crate::proto_bufs;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use tokio::sync::OnceCell;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::blocks;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};

use prost::Message; 

use crate::resources::validators;
use crate::utils;
use crate::resources;
use crate::resources::signatures;
// 1. Define the custom behavior combining Gossipsub and MDNS


static SELF_SIGN_KEY_LOADED: OnceLock<Option<signatures::EntityKeyML>> = OnceLock::new();
static SELF_SIGN_KEY: OnceCell<Option<Resource>> = OnceCell::const_new();

pub async fn sign_key() -> &'static Option<Resource> {


    SELF_SIGN_KEY
    .get_or_init(|| async {
        let args: Vec<String> = env::args().collect();
        let key_grid = &args[4];
        let key_exists = Resource::find_resource(key_grid.to_string()).await;
        if key_exists.is_none(){
            println!("NOOKEEEEE!");
        }
        else{
            println!("Key found!");
        }
        Resource::find_resource(key_grid.to_string()).await
    })
    .await

}



pub async fn sign_resource(data: Vec::<u8>) -> Vec::<u8> {

    if let Some(key) = sign_key().await{
        let loaded_key = signatures::load_key(&key);
        return loaded_key.unwrap().generate_signature(data.clone());
    }
    println!("Key not found...");
    return vec![];
    
    
}