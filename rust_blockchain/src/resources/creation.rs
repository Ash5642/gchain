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

use crate::utils;
use crate::resources::signatures;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};
use crate::resources::structures;

use prost::Message; 


use async_recursion::async_recursion;
use crate::resources;
use super::FieldValue;


impl Resource{
    pub fn create_resource(geid: &str, r#type: &str, identifier: &str, field_values: &Vec::<Vec::<u8>>, state: &mut state::StateSession) -> Option<Resource>{
        let mut fields: Vec::<Field> = Vec::new();
        let mut type_template = Resource::type_str_to_uint(r#type);
        let mut resource_type: u32 = 0;
        if let Some(template) = type_template{
            if template.fields.len()!=field_values.len(){
                return None
            }
            resource_type = template.id;
            
            for (pos, field) in template.fields.into_iter().enumerate(){
                if field_values[pos].clone().len() == 0{
                    if field.required{
                        return None;
                    }
                }
                fields.push(
                    Field{
                        key: field.key.to_string(),
                        r#type: field.r#type.to_string(),
                        value: field_values[pos].clone(),
                        nonce: state.rand(),
                        id: field.id,
                    }
                );
            }
        }


        let mut new_resource = Resource{
            payload: Some(
                Payload{
                    grid: format!("{geid}[{type}]={identifier}"),
                    r#type: resource_type,
                    fields: fields,
                    action: "GENERATED".to_string(),
                    nonce: state.rand()
                },
            ),
            proof: Vec::new(),
            signatures: Vec::new(),
            status: 0
        };
        return Some(new_resource)
    }

    pub async fn create_transaction(sender: &str, reciever: &str, amt: u64, identifier: &str) -> Resource{
        let transaction = Resource{
            payload: Some(
                Payload{
                    grid:format!("{reciever}[CASH]={identifier}"),
                    r#type: structures::ResourceTypes::Transaction.id,
                    action: "GENERATED".to_string(),
                    fields: vec![
                        Field{
                            key: "from".to_string(),
                            r#type: "gid".to_string(),
                            value: sender.bytes().collect(),
                            nonce: Vec::new(),
                            id: 1,
                        },
                        Field{
                            key: "to".to_string(),
                            r#type: "gid".to_string(),
                            value: reciever.bytes().collect(),
                            nonce: Vec::new(),
                            id: 2,
                        },
                        Field{
                            key: "to".to_string(),
                            r#type: "uint".to_string(),
                            value: amt.to_le_bytes().to_vec(),
                            nonce: Vec::new(),
                            id: 3,
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
    pub async fn create_reward(reciever: &str, amt: u64, identifier: &str) -> Resource{

        let transaction = Resource{
            payload: Some(
                Payload{
                    grid:format!("{reciever}[CASH]={identifier}"),
                    r#type: structures::ResourceCodes::Transaction,
                    action: "ADDED".to_string(),
                    fields: vec![
                        Field{
                            key: "from".to_string(),
                            r#type: "gid".to_string(),
                            value: "CHAIN".bytes().collect(),
                            nonce: Vec::new(),
                            id: 1,
                        },
                        Field{
                            key: "to".to_string(),
                            r#type: "gid".to_string(),
                            value: reciever.bytes().collect(),
                            nonce: Vec::new(),
                            id: 2,
                        },
                        Field{
                            key: "amt".to_string(),
                            r#type: "uint".to_string(),
                            value: amt.to_le_bytes().to_vec(),
                            nonce: Vec::new(),
                            id: 3
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