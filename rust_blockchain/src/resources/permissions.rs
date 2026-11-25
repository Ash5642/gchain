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
use crate::resources::structures;
use crate::utils;
use crate::resources::signatures;
use crate::proto_bufs::ChainMessage as ChainMessage;
use crate::resources::structures::PermissionCode as PermissionCode;

use sha2::{Sha256, Digest};

use prost::Message; 


use async_recursion::async_recursion;
use crate::resources;

impl Resource{
    pub fn check_permission(&self, action: usize, geid: &str) -> bool{
        let entity = entity::Entity::get_entity(geid);
        let resource_entity = self.get_entity();
        let parent_entity = resource_entity.get_parent();
        let resource_structure = self.get_structure();
        println!("Loaded as type: {}", resource_structure.name);
        if let Some(permissions) = resource_structure.permissions{
            if permissions[action][PermissionCode::Direct]{
                if entity == resource_entity{
                    println!("Direct permission!");
                    return true
                }
            }
            if permissions[action][PermissionCode::Parent]{
                if entity == parent_entity{
                    println!("Indirect permission!");
                    return true
                }
            }
            if permissions[action][PermissionCode::Executable]{
                if entity == parent_entity{
                    println!("Indirect permission!");
                    return true
                }
            }
        }

        
        true
    }
}