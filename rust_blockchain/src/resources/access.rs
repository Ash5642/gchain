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

use prost::Message; 

use async_recursion::async_recursion;
use crate::resources;
use super::FieldValue;
impl Resource{
    pub fn find_signature(&self, grid: &String) -> Option<Resource>{
        let mut result: Option<Resource> = None;
        for mut signature in &self.signatures{
            if signature.payload.clone().unwrap().grid == *grid{
                result = Some(signature.clone());
            }
        }
        return result;
    }
    pub fn get_field_bytes(&self, key: &str) -> Vec::<u8>{
        for field in self.payload.as_ref().unwrap().fields.clone(){
            if field.key == key || key == "*"{
                return field.value.clone()
            }
        }
        return Vec::new();
    }


    pub fn get_field(&self, key: &str) -> FieldValue{
        for field in self.payload.as_ref().unwrap().fields.clone(){
            if field.key == key || key == "*"{

                return match field.r#type.as_str() {
                    "bytes" => {
                        let encoded = hex::encode(field.value.clone());
                        return FieldValue::Bytes(field.value.clone());
                    },
                    "uint" => {
                        let encoded = hex::encode(field.value.clone());
                        return FieldValue::Number(u64::from_le_bytes(field.value.clone().try_into().expect("INCORRLEN")));
                    },
                    
                    "gid"|"gid:key"|"ARR:gid"|"str"  => {
                        return FieldValue::Text(String::from_utf8(field.value.clone()).unwrap())
                    },
                    _=>{
                        return FieldValue::Flag(true)
                    },
                }
            }
        }
        return FieldValue::Text(String::from("NOT_FOUND"));
    }

    pub fn get_text_field(&self, key: &str) -> String{
        
        let str_enum = self.get_field(key);
        //////println!("ETETETETETE {}", key);
        let FieldValue::Text(str_result) = str_enum else { 
            //////println!("{:?}", str_enum);
            //////println!("UHOH!");
            return "".to_string(); 
        };
        //////println!("UUHUHUHUH {}", str_result);
        return str_result;
    }

    pub fn get_int_field(&self, key: &str) -> u64{
        
        let str_enum = self.get_field(key);
        let FieldValue::Number(str_result) = str_enum else { 
            //////println!("UHOH!");
            return u64::MAX; 
        };
        //////println!("UUHUHUHUH {}", str_result);
        return str_result;
    }

    pub fn get_byte_field(&self, key: &str) -> Option<Vec::<u8>>{
        
        let str_enum = self.get_field(key);
        let FieldValue::Bytes(str_result) = str_enum else { 
            return None; 
        };
        return Some(str_result);
    }

    pub fn check_validity(&self) -> bool{
        true
    }  
}