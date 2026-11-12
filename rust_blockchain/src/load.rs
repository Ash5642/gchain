use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
use std::fs;
use std::env;
use std::io;
use crate::mempool_actions;
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
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::consensus;
use crate::mempool;
use crate::proto_bufs;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::blocks;
use crate::headers;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};
use crate::crypt;
use prost::Message; 
use crate::block_db;
use crate::bloom;
use crate::validators;
use crate::utils;
use crate::resources;
use crate::resources::ChainResource;
// 1. Define the custom behavior combining Gossipsub and MDNS


fn get_geid() -> &'static str {
    let args: Vec<String> = env::args().collect();
    let self_geid = &args[3];
    SELF_GEID.get_or_init(|| {
        format!("{self_geid}")
    })
}
