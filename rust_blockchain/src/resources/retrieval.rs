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
use crate::proto_bufs;
use crate::resources::ResourcePool;
use crate::consensus;
impl Resource{
    pub async fn exists(&self) -> Option<bool>{
        let mut resource_pool = ResourcePool.write().await;
        let mut election_list = consensus::ElectionList.read().await;
        for election in election_list.iter(){
            for node in election.nodes.iter(){
                for stake in node.stakes.iter(){
                    if *stake == self.payload.as_ref().unwrap().grid{
                        return Some(false);
                    }
                }
            }
        }
        for resource in resource_pool.iter(){
            if resource.payload.clone().unwrap().grid == self.payload.clone().unwrap().grid{
                return Some(true)
            }
        }
        let mut header_list = blocks::HeadersList.write().await;


        
        
        for header in header_list.iter_mut().rev(){
            let include_bloom_result = header.check_bloom(format!("+{}",self.payload.clone().unwrap().grid));
            let revoke_bloom_result = header.check_bloom(format!("-{}",self.payload.clone().unwrap().grid));
            
            if include_bloom_result || revoke_bloom_result{                
                let head_clone = header.clone();
                let self_clone = self.clone();
                let mut db_result = false;
                if include_bloom_result{
                    db_result = blocks::Header::check_Database(header, self_clone.payload.clone().unwrap().grid).await;
                }
                else{
                    let mut db_result = blocks::Header::check_Database_revoke(header, self_clone.payload.clone().unwrap().grid).await;
                    if db_result{
                        return Some(false);
                    }
                }
                if db_result{
                    return Some(true)
                }

            }else{
                ////////println!("Bloom mismatch");
                continue
            }
        }
        None
    }
    pub async fn revoked(&self) -> Option<u32>{

        let mut header_list = blocks::HeadersList.write().await;
        for header in header_list.iter_mut().rev(){
            let revoke_bloom_result = header.check_bloom(format!("-{}",self.payload.clone().unwrap().grid));
            if revoke_bloom_result{
                let mut db_result = blocks::Header::check_Database_revoke(header, self.payload.clone().unwrap().grid).await;
                if db_result{
                    return Some(header.num);
                }
            }
        }
        None
    }
    pub async fn grid_exists(grid: String) -> bool{
        ////////println!("Checking existence");
        let mut resource_pool = ResourcePool.write().await;
        let mut header_list = blocks::HeadersList.write().await;
        
        for header in header_list.iter_mut(){
            let bloom_result = header.check_bloom(format!("+{}", grid.clone()));
            ////////println!("Bloom result: {}", bloom_result);
            if bloom_result{
                ////////println!("Match in block: {}", header.num);
                let head_clone = header.clone();
                let db_result = blocks::Header::check_Database(header, grid.clone()).await;
                if db_result{
                    return true
                }

            }else{
                ////////println!("Bloom mismatch");
                continue
            }
        }
        false
    }

    pub async fn grid_in_mempool(grid: &str) -> bool{
        let mut resource_pool = ResourcePool.read().await;
        for resource in resource_pool.iter(){
            if resource.payload.as_ref().unwrap().grid == grid{
                return true;
            }
        }
        return false;
    }

    pub async fn grid_status(grid: &str) -> Vec::<proto_bufs::GridStatus>{
        let mut result: Vec::<proto_bufs::GridStatus> = Vec::new();

        if Resource::grid_in_mempool(grid).await{
            result.push(
                proto_bufs::GridStatus{
                    status:"MEMPOOL".to_string(),
                    block: 0 as u64
                }
            )
        }
        let mut header_list = blocks::HeadersList.write().await;
        for header in header_list.iter_mut().rev(){
            let bloom_add_result = header.check_bloom(format!("+{}", grid.clone()));
            let bloom_revoke_result = header.check_bloom(format!("-{}", grid.clone()));
            if bloom_revoke_result{
                if blocks::Header::check_Database_revoke(header, grid.to_string()).await{
                    result.push(
                        proto_bufs::GridStatus{
                            status: "REVOKE".to_string(),
                            block: header.num as u64
                        }
                    );
                }
            }
            if bloom_add_result{
                if blocks::Header::check_Database(header, grid.to_string()).await{
                    result.push(
                        proto_bufs::GridStatus{
                            status: "CHAIN".to_string(),
                            block: header.num as u64
                        }
                    );
                    return result;
                }

            }else{
                continue
            }
        }
        result
    }


    pub async fn find_resource(grid: String) -> Option<Resource>{
        let mut header_list = blocks::HeadersList.write().await;
        let mut revoke_proof: Option<proto_bufs::MerkleProof> = None;
        let mut result: Option<Resource> = None;

        for header in header_list.iter_mut().rev(){

            let include_header_bloom = header.check_bloom(format!("+{}", grid.clone()));
            let revoke_header_bloom = header.check_bloom(format!("-{}", grid.clone()));
            if revoke_header_bloom{
                let mut header_revocation = blocks::Header::fetch_revoke(header, grid.clone()).await;
                if !header_revocation.as_ref().is_none(){
                    revoke_proof = Some(
                        proto_bufs::MerkleProof{
                            r#type: format!("REVOKE"),
                            hashes: Vec::new(),
                            block: header.num,
                            grid: header_revocation.as_ref().unwrap().to_string()
                        }
                    )
                }
                else{
                    //////println!("False positive!");
                }

            }
            if include_header_bloom == false{
                continue
            }
            result = blocks::Header::fetch_database(header, grid.clone()).await;
            if !result.is_none(){
                if let Some(proof) = revoke_proof.take() {
                    let result_borrowed = result.as_mut().unwrap();
                    result_borrowed.status = 2;
                    result_borrowed.proof.push(proof); // Safely moves the taken proof
                }
            }
        }
        return result
    }
}