use super::Election as Election;
use super::ElectionList as ElectionList;

use super::Node as Node;

use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;
use crate::proto_bufs;
use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;
use prost::Message;
use crate::blocks;
use crate::resources::validators;
use std::fs;
use crate::blocks::merkle::MerkleNode;

use crate::state;


use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use crate::resources;
use rand::RngExt;
use rand::Rng;
use sha2::{Sha256, Digest};
use crate::self_node;
use std::env;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;
use crate::resources::signatures;
use std::sync::atomic::{AtomicBool, Ordering};
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};



impl Election{

    pub async fn grind(target: Vec::<u8>, window_size: u32, stop: Arc<AtomicBool>) -> Option<[u8; 32]>{
        let mut grind_nonce: [u8; 32] = [0u8; 32];
        while !stop.load(Ordering::Relaxed){
            rand::rng().fill_bytes(&mut grind_nonce);
            let distance = utils::xor_bit_distance(&grind_nonce, &target[..]);
            if distance < window_size{
                println!("Found!");
                return Some(grind_nonce.try_into().expect("Error!"));
            }
        }
        println!("PREMATURW");
        return None;
    }


    pub async fn get_target(identifier: &Vec::<u8>) -> [u8; 32]{
        let state_root = state::get_root().await;
        return state_root.try_into().expect("Root error!");
    }


    pub async fn set_target(index: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        Election::send_consensus_message(index, "TARGET", &Vec::new(), tx_msg.clone()).await;
        Election::select_PoS_validator(index, tx_msg).await;
    }


    pub async fn select_PoS_validator(identifier: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>){
        let mut election_list = ElectionList.read().await;
        let mut election = match election_list.get(identifier) {
            Some(e) => e,
            None => return,
        };
        let target = Election::get_target(identifier).await;
        println!("Target: {:?}", hex::encode(target));
        let mut closest_distance: u32 = 256;
        let mut closest_geid: Option<&str> = None;
        for (node_geid, node) in election.nodes.iter(){
            let distance = utils::xor_bit_distance(&node.value.unwrap(), &target);
            if distance<closest_distance{
                closest_distance = distance;
                closest_geid = Some(node_geid);
            }
        }
        println!("Winner: {:?} at {:?}", closest_geid, closest_distance);
        println!("{}, {}",  closest_geid.unwrap(), crate::get_geid().to_string());
        if closest_geid.unwrap() == crate::get_geid().to_string(){
            println!("Proposing!");
            drop(election);
            drop(election_list);
            Election::propose_block(identifier, tx_msg).await;
        }
        
    }


    

    pub async fn select_validator(index: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) {
        let election_list = ElectionList.read().await;
        let election = match election_list.get(index) {
            Some(e) => e,
            None => return,
        };
        let target = Election::get_target(index).await;
        println!("Target: {:?}", hex::encode(target));
        if election.PoW_enabled{
            let mut closest_distance: u32 = 256;
            let mut closest_geid: Option<&str> = None;
            for (node_geid, node) in election.nodes.iter(){
                let distance = utils::xor_bit_distance(&node.value.unwrap(), &target);
                if distance<closest_distance{
                    closest_distance = distance;
                    closest_geid = Some(node_geid);
                }
            }
            println!("Winner: {:?} at {:?}", closest_geid, closest_distance);
            if closest_geid.unwrap() == crate::get_geid(){
                println!("Won!");
            }
        }
        else{
            let stop = election.stop_grind_flag.clone();
            let mut grind_thread = tokio::spawn(
                async move{
                    Election::grind(target.to_vec(), 90, stop).await
                }
            );
            match grind_thread.await {
                Ok(Some(val)) => {
                    println!("Win!");
                    return;
                }
                _ => {
                    return;
                }
            }
        }



        
    }


}