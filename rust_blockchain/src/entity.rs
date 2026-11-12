use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
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
use crate::mempool;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs;

use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::Field as Field;

use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};
use crate::crypt;
use prost::Message; 
use crate::block_db;
use crate::bloom;
use crate::validators;
use crate::headers;
use std::sync::{LazyLock, Arc};
use tokio::sync::RwLock;
#[derive(Debug)]
#[derive(PartialEq)]
#[derive(Clone)]

pub struct EntityToken{
    pub r#type: String,
    pub name: String,
}
#[derive(PartialEq)]
#[derive(Debug)]
#[derive(Clone)]

pub struct Entity{
    pub name_list:Vec::<EntityToken>,
}



impl Entity{
    pub fn dclone(&self) -> Entity{
        let mut new_name_list: Vec::<EntityToken> = Vec::new();
        for name in &self.name_list{
            new_name_list.push(
                EntityToken{
                    r#type: name.r#type.clone(),
                    name: name.name.clone(),
                }
            );
        }
        let new_entity = Entity{
            name_list: new_name_list
        };
        return new_entity;

    }
    pub fn get_entity(grid: String) -> Entity {
        let mut entity = Entity{
            name_list: Vec::new()
        };
        let geid = grid.split('[').next().unwrap_or("");

        let mut anchors: String;
        let mut non_anchors: String;


        if geid.contains("::"){
            let anchor_split: Vec::<&str> = geid.split("::").collect();
            anchors = anchor_split[0].to_string();
            non_anchors = anchor_split[1].to_string();
        }
        else{
            anchors = geid.to_string();
            non_anchors = String::from("");
        }


        let mut anchor_tokens: Vec::<&str> = anchors.split(":").collect();
        let mut non_anchor_tokens: Vec::<&str> = Vec::new();
        if non_anchors.as_str() != ""{
            non_anchor_tokens = non_anchors.split(":").collect();
        }
       




        for anchor_token in anchor_tokens{
            entity.name_list.push(
                EntityToken{
                    r#type: String::from("ANCHOR"),
                    name: anchor_token.to_string()
                }
            )
        }
        for non_anchor_token in non_anchor_tokens{
            entity.name_list.push(
                EntityToken{
                    r#type: String::from("NOT_ANCHOR"),
                    name: non_anchor_token.to_string()
                }
            )
        }
        return entity;
    }

    pub fn has_anchor(&self) -> bool{
        if self.name_list.len() > 1{
            return true;
        }
        return false
    }

    pub fn get_parent(&self) -> Option<Entity>{

        let mut parent_entity = self.clone();
        if self.name_list.len()<2{
            return None;
        }
        parent_entity.name_list = self.name_list[..self.name_list.len() - 1].to_vec();
        return Some(parent_entity);
    }

    pub fn has_parent(&self) -> bool{
        return self.name_list.len() > 1;
    }

    pub fn get_geid(&self) -> String{
        let mut geid = String::from("");
        let mut previous = String::from("*");
        for token in self.name_list.clone(){
            if token.r#type.as_str() != "ANCHOR" && previous.as_str() == "ANCHOR"{
                geid.push_str("::");
            }
            else if previous.as_str() != "*"{
                geid.push_str(":");
            }
            geid.push_str(&token.name);
            previous = token.r#type;
        }
        return geid;
    }

}


