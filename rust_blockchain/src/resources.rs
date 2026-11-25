use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};

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
use crate::mempool;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs;
use crate::entity;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::Field as Field;
use crate::vm;
use crate::state;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};

use prost::Message; 


use crate::blocks;
use std::sync::{LazyLock, Arc};
use crate::consensus;
use tokio::sync::RwLock;
pub mod structures;
pub mod validators;
pub mod creation;
pub mod retrieval;
pub mod encoding;
pub mod access;
pub mod signatures;
pub mod cache;
pub mod permissions;

use crate::resources::structures::ResourceType;
#[derive(Debug)]

pub enum FieldValue {
    Text(String),
    Bytes(Vec::<u8>),
    Number(u64),
    Flag(bool),
}

pub static ResourcePool: LazyLock<Arc<RwLock<Vec<Resource>>>> = LazyLock::new(|| Arc::new(RwLock::new(vec![])));

impl Resource{
    pub fn greet(&self) -> String {
        format!("Hello, {}!", self.payload.clone().unwrap().grid)
    }

    pub async fn add_resource(&self){
        let mut resource_pool = ResourcePool.write().await;
        resource_pool.push(self.clone());
        drop(resource_pool)
    }

    pub fn type_str_to_uint(r#type: &str) -> Option<ResourceType>{
        match r#type{
            "key" => {
                return Some(structures::ResourceTypes::Key)
            }
            "record"|"RECORD"|"Record" => {
                return Some(structures::ResourceTypes::Record)
            }

            _ => {
                return None
            }
        }
    }
    pub fn get_structure(&self) -> &ResourceType{
        let resource_code: usize = self.payload.as_ref().unwrap().r#type as usize;
        if resource_code < structures::ResourceTypes::by_type.len(){
            return &structures::ResourceTypes::by_type[resource_code];
        }
        return &structures::ResourceTypes::Generic
    }


}

fn xor_vec_in_place(a: &mut [u8], b: &[u8]) {
    assert_eq!(a.len(), b.len(), "Vectors must be of equal length");

    for (x, &y) in a.iter_mut().zip(b.iter()) {
        *x ^= y;
    }
}
