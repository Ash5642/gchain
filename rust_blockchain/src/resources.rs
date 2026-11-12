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
use crate::entity;
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
use crate::consensus;
use tokio::sync::RwLock;
#[derive(Debug)]

pub enum FieldValue {
    Text(String),
    Bytes(Vec::<u8>),
    Number(i32),
    Flag(bool),
}

pub trait ChainResource {
    fn greet(&self) -> String;
    fn get_entity(&self) -> entity::Entity;
    fn to_bytes(&self) -> Vec::<u8>;
    fn to_hash(&self) -> Vec::<u8>;
    fn hash(&self) -> Vec::<u8>;
    fn get_field(&self, key: &str) -> FieldValue;
    fn check_validity(&self) -> bool;
    async fn exists(&self) -> Option<bool>;
    async fn is_locked(grid: String) -> bool;
    async fn revoked(&self) -> Option<u32>;
    async fn add_resource(&self);
    async fn validate_add(&self) -> bool;
    async fn find_resource(grid: String) -> Option<Resource>;
    fn get_text_field(&self, key: &str) -> String;
    fn get_byte_field(&self, key: &str) -> Option<Vec::<u8>>;
    fn get_int_field(&self, key: &str) -> i32;
    fn find_signature(&self, grid: &String) -> Option<Resource>;
    async fn grid_exists(grid: String) -> bool;
    async fn grid_status(grid: &str) -> Vec::<proto_bufs::GridStatus>;
    async fn grid_in_mempool(grid: &str) -> bool;
    fn create_resource(geid: &str, r#type: &str) -> Resource;
    
}

pub static ResourcePool: LazyLock<Arc<RwLock<Vec<Resource>>>> = LazyLock::new(|| Arc::new(RwLock::new(vec![])));


fn xor_vec_in_place(a: &mut [u8], b: &[u8]) {
    assert_eq!(a.len(), b.len(), "Vectors must be of equal length");

    for (x, &y) in a.iter_mut().zip(b.iter()) {
        *x ^= y;
    }
}
impl ChainResource for Resource{
    fn greet(&self) -> String {
        format!("Hello, {}!", self.payload.clone().unwrap().grid)
    }

    fn to_bytes(&self) -> Vec::<u8> {
        format!("Hello, {}!", self.payload.clone().unwrap().grid);
        let mut buf = Vec::new();
        self.payload.clone().unwrap().encode(&mut buf);
        return buf;
    }
    fn get_entity(&self) -> entity::Entity {
        return entity::Entity::get_entity(self.payload.clone().unwrap().grid)
    }

    fn to_hash(&self) -> Vec::<u8>{
        
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

    fn hash(&self) -> Vec::<u8>{
        return self.to_hash();
    }

    async fn add_resource(&self){
        let mut resource_pool = ResourcePool.write().await;
        resource_pool.push(self.clone());
        drop(resource_pool)
    }

    fn get_field(&self, key: &str) -> FieldValue{
        for field in self.payload.as_ref().unwrap().fields.clone(){
            if field.key == key || key == "*"{

                return match field.r#type.as_str() {
                    "bytes" => {
                        ////////////////println!("TEXT"); 
                        let encoded = hex::encode(field.value.clone());
                        //////////////println!("VAL {} {}", field.key, encoded);
                        return FieldValue::Bytes(field.value.clone());
                    },
                    "int" => {
                        ////////////////println!("TEXT"); 
                        let encoded = hex::encode(field.value.clone());
                        //////////////println!("VAL {} {}", field.key, encoded);
                        return FieldValue::Number(i32::from_le_bytes(field.value.clone().try_into().expect("INCORRLEN")));
                    },
                    
                    "gid"|"gid:key"|"ARR:gid"|"str"  => {
                        //////////////println!("VAL {} {}", field.key, String::from_utf8(field.value.clone()).unwrap());
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

    fn get_text_field(&self, key: &str) -> String{
        
        let str_enum = self.get_field(key);
        println!("ETETETETETE {}", key);
        let FieldValue::Text(str_result) = str_enum else { 
            println!("{:?}", str_enum);
            println!("UHOH!");
            return "".to_string(); 
        };
        println!("UUHUHUHUH {}", str_result);
        return str_result;
    }

    fn get_int_field(&self, key: &str) -> i32{
        
        let str_enum = self.get_field(key);
        let FieldValue::Number(str_result) = str_enum else { 
            println!("UHOH!");
            return -1; 
        };
        println!("UUHUHUHUH {}", str_result);
        return str_result;
    }

    fn get_byte_field(&self, key: &str) -> Option<Vec::<u8>>{
        
        let str_enum = self.get_field(key);
        let FieldValue::Bytes(str_result) = str_enum else { 
            return None; 
        };
        return Some(str_result);
    }

    fn check_validity(&self) -> bool{
        true
    }  


    async fn is_locked(grid: String) -> bool{

        return false;
    }
    
    async fn exists(&self) -> Option<bool>{
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
        let mut header_list = headers::HeadersList.write().await;


        
        
        for header in header_list.iter_mut().rev(){
            let include_bloom_result = header.check_bloom(format!("+{}",self.payload.clone().unwrap().grid));
            let revoke_bloom_result = header.check_bloom(format!("-{}",self.payload.clone().unwrap().grid));
            
            if include_bloom_result || revoke_bloom_result{                
                let head_clone = header.clone();
                let self_clone = self.clone();
                let mut db_result = false;
                if include_bloom_result{
                    db_result = headers::Header::check_Database(header, self_clone.payload.clone().unwrap().grid).await;
                }
                else{
                    let mut db_result = headers::Header::check_Database_revoke(header, self_clone.payload.clone().unwrap().grid).await;
                    if db_result{
                        return Some(false);
                    }
                }
                if db_result{
                    return Some(true)
                }

            }else{
                //println!("Bloom mismatch");
                continue
            }
        }
        None
    }
    async fn revoked(&self) -> Option<u32>{

        let mut header_list = headers::HeadersList.write().await;
        
        for header in header_list.iter_mut().rev(){
            let revoke_bloom_result = header.check_bloom(format!("-{}",self.payload.clone().unwrap().grid));
            if revoke_bloom_result{
                let mut db_result = headers::Header::check_Database_revoke(header, self.payload.clone().unwrap().grid).await;
                if db_result{
                    return Some(header.num);
                }
            }
        }
        None
    }
    async fn grid_exists(grid: String) -> bool{
        //println!("Checking existence");
        let mut resource_pool = ResourcePool.write().await;
        let mut header_list = headers::HeadersList.write().await;
        
        for header in header_list.iter_mut(){
            let bloom_result = header.check_bloom(format!("+{}", grid.clone()));
            //println!("Bloom result: {}", bloom_result);
            if bloom_result{
                //println!("Match in block: {}", header.num);
                let head_clone = header.clone();
                let db_result = headers::Header::check_Database(header, grid.clone()).await;
                if db_result{
                    return true
                }

            }else{
                //println!("Bloom mismatch");
                continue
            }
        }
        false
    }

    async fn grid_in_mempool(grid: &str) -> bool{
        let mut resource_pool = ResourcePool.read().await;
        for resource in resource_pool.iter(){
            if resource.payload.as_ref().unwrap().grid == grid{
                return true;
            }
        }
        return false;
    }

    async fn grid_status(grid: &str) -> Vec::<proto_bufs::GridStatus>{
        let mut result: Vec::<proto_bufs::GridStatus> = Vec::new();
        if Resource::is_locked(grid.to_string()).await{
            result.push(
                proto_bufs::GridStatus{
                    status: "LOCKED".to_string(),
                    block: 0 as u64
                }
            );
        }
        if Resource::grid_in_mempool(grid).await{
            result.push(
                proto_bufs::GridStatus{
                    status:"MEMPOOL".to_string(),
                    block: 0 as u64
                }
            )
        }
        let mut header_list = headers::HeadersList.write().await;
        for header in header_list.iter_mut().rev(){
            let bloom_add_result = header.check_bloom(format!("+{}", grid.clone()));
            let bloom_revoke_result = header.check_bloom(format!("-{}", grid.clone()));
            if bloom_revoke_result{
                if headers::Header::check_Database_revoke(header, grid.to_string()).await{
                    result.push(
                        proto_bufs::GridStatus{
                            status: "REVOKE".to_string(),
                            block: header.num as u64
                        }
                    );
                }
            }
            if bloom_add_result{
                if headers::Header::check_Database(header, grid.to_string()).await{
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

    async fn validate_add(&self) -> bool{
        let exists = self.exists().await;
        let in_mempool = Resource::grid_in_mempool(&self.payload.as_ref().unwrap().grid).await;
        if exists == None || in_mempool{
            return true
        }
        return false;
    }



    async fn find_resource(grid: String) -> Option<Resource>{
        let mut header_list = headers::HeadersList.write().await;
        let mut revoke_proof: Option<proto_bufs::MerkleProof> = None;
        let mut result: Option<Resource> = None;
        let locked = Resource::is_locked(grid.clone()).await;

        for header in header_list.iter_mut().rev(){

            let include_header_bloom = header.check_bloom(format!("+{}", grid.clone()));
            let revoke_header_bloom = header.check_bloom(format!("-{}", grid.clone()));
            if revoke_header_bloom{
                let mut header_revocation = headers::Header::fetch_revoke(header, grid.clone()).await;
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
                    println!("False positive!");
                }

            }
            if include_header_bloom == false{
                continue
            }
            result = headers::Header::fetch_database(header, grid.clone()).await;
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

    fn find_signature(&self, grid: &String) -> Option<Resource>{
        let mut result: Option<Resource> = None;
        for mut signature in &self.signatures{
            if signature.payload.clone().unwrap().grid == *grid{
                result = Some(signature.clone());
            }
        }
        return result;
    }


    fn create_resource(geid: &str, r#type: &str) -> Resource{
        let mut new_resource = Resource{
            payload: Some(
                Payload{
                    grid: format!("{geid}[{type}]=ABS123"),
                    r#type: r#type.to_string(),
                    fields: Vec::new(),
                    action: "".to_string(),
                    nonce: Vec::new()
                },
            ),
            proof: Vec::new(),
            signatures: Vec::new(),
            status: 0
        };
        return new_resource
    }


}


