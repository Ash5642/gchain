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
use crate::state;
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
use crate::entity;
use crate::blocks;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::Field as Field;
use crate::proto_bufs;
use crate::utils;
use crate::resources::signatures;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};

use prost::Message; 


use async_recursion::async_recursion;
use crate::resources;
use super::FieldValue;
use resources::structures::ResourceTypes as ResourceTypes;
use crate::resources::xor_vec_in_place;


impl Resource{

    pub fn to_bytes(&self) -> Vec::<u8> {
        format!("Hello, {}!", self.payload.clone().unwrap().grid);
        let mut buf = Vec::new();
        self.payload.clone().unwrap().encode(&mut buf);
        return buf;
    }
    pub fn get_entity(&self) -> entity::Entity {
        return entity::Entity::get_entity(&self.payload.as_ref().unwrap().grid)
    }

    pub fn to_hash(&self) -> Vec::<u8>{
        
        let mut result;
        let mut bin: Vec::<u8> = Vec::new();

        let mut field_hash_xor: Vec<u8> = vec![0u8; 32];
        for field in self.payload.clone().unwrap().fields{
            let mut field_encoded = Vec::new();
            field.encode(&mut field_encoded);
            let mut hasher = Sha256::new();
            hasher.update(field_encoded);
            let field_hash = hasher.finalize().to_vec();
            xor_vec_in_place(&mut field_hash_xor, &field_hash);
        }

        let field_hash_encoded = hex::encode(&field_hash_xor);


        let hash_resource = proto_bufs::HashPayload{
            grid: self.payload.clone().unwrap().grid,
            r#type: self.payload.clone().unwrap().r#type,
            action:  self.payload.clone().unwrap().action,
            fields: field_hash_xor.clone(),
            nonce:  self.payload.clone().unwrap().nonce
        };
        hash_resource.encode(&mut bin);
        let mut final_hasher = Sha256::new();
        final_hasher.update(&bin);
        result = final_hasher.finalize().to_vec();
        return result;
    }

    pub fn hash(&self) -> Vec::<u8>{
        return self.to_hash();
    }
    
}