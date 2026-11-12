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
use crate::state;
use rand::Rng;
use std::error::Error;
use crate::rules;
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
use crate::headers;

use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::Field as Field;

use crate::utils;
use crate::signatures;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};
use crate::crypt;
use prost::Message; 
use crate::block_db;
use crate::bloom;
use crate::cryptography;
use async_recursion::async_recursion;
use crate::resources::ChainResource;
use crate::resources;


pub async fn verify_proof(mut item: Resource) -> bool{
    //println!("Validating: {}", item.payload.as_ref().unwrap().grid);
    let result = verify_resource(item.clone(), None).await;
    //println!("{}: {}!", item.clone().payload.unwrap().grid.as_str(), result.unwrap_or(String::from("INVALID")));
    return true

}


#[async_recursion]
pub async fn verify_resource(mut item: Resource, parent: Option<Resource>) -> Option<String>{
    let mut decided = false;
    let mut result = false;
    let entity = entity::Entity::get_entity(item.clone().payload.unwrap().grid);
    let parent = entity.get_parent();
    println!("{}\t{}", item.payload.as_ref().unwrap().grid, item.payload.as_ref().unwrap().r#type);

    if parent.is_none(){
        //println!("Root entity element!");
        let geid = entity.get_geid();
        let geid_record = format!("{geid}[RECORD]");
        //println!("Searching for record for: {}", geid_record);
        let mut record_exists = false;
        let record_list = crate::Resource::grid_status(&geid_record).await;;
        for record in record_list.iter(){
            //println!("Record: {:?}", record);
            if record.status == "CHAIN"{
                record_exists = true;
            }
        }
        if !record_exists{
            //println!("Entity has no on chain record!");
        }
    }

    match item.payload.as_ref().unwrap().r#type.as_str(){
        "CASH" => {
            println!("Cash valid!");
            let recipient = item.get_text_field("rx");
            let amount = item.get_int_field("amt");
            let sender = entity.get_geid();
            println!("{}", sender);
            println!("{}", recipient);
            println!("{}", (amount as u64));
            let sender_balance = state::read_record(&sender).await;
            if sender_balance.is_none() || sender_balance.unwrap().balance < (amount as u64){
                println!("HELLNOOOOOOOOO");
                return None
            }
            let recipient_balance = state::read_record(&recipient).await;
            if recipient_balance.is_none(){
                println!("Recipient does not exisy!");
                return None;
            }
            
            println!("Transaction valid!");
        }
        _ => {}
    }

    for proof in &item.proof{
        let raw_validity = utils::check_merkle_proof(proof.hashes.clone(), item.to_hash());
        if proof.grid.as_str() == ""{
            let root_valid = headers::Header::match_header(proof.hashes.last().unwrap().to_vec()).await;
            if root_valid == false{
                //return None;
            }
            ////println!("Validated by chain inclusion");
           //return Some(String::from("CHAIN"));
        }
        else{
            let included_lroot = item.find_signature(&proof.grid);
            if included_lroot.is_none(){
                //return None;
            }
            if *proof.hashes.last().unwrap() != included_lroot.clone().unwrap().get_byte_field("resource_hash").unwrap(){
                //return None;
            }
            //return Some(String::from("CHAIN"));
        }
    }
    println!("Direct check!");
    let entity = item.get_entity();
    let parent = entity.get_parent();   
    if !parent.is_none(){
        match item.payload.as_ref().unwrap().r#type.as_str(){
            "KEY"|"RECORD" => {
                //println!("Key validation!");
                let mut parent_auth = false;
                for included in &item.signatures{
                    match included.payload.as_ref().unwrap().r#type.as_str(){
                        "SIGNATURE"|"LOCALROOT"|"LROOT" => {
                            if included.get_entity() == *parent.as_ref().unwrap(){
                                parent_auth = true;
                            }
                        }
                        _ => {

                        }
                    }
                }
                if !parent_auth{
                    return None;
                }
            }
            "SIGNATURE"|"LOCALROOT"|"LROOT" => {
                let signing_key = item.get_text_field("key");
                let signing_key_match = item.find_signature(&signing_key);
                if signing_key_match.is_none(){
                    //println!("Key not included!");
                }
                else{
                    //println!("Key {} included!", signing_key);
                }
            }
            _ =>{

            }
        }
    }
    let mut included_handles = vec![];
    for included in &item.signatures{
        let included_clone = included.clone();
        let item_clone = item.clone();
        let included_validity = task::spawn(
            async move {
                return verify_resource(included_clone, Some(item_clone)).await
            }
        );
        included_handles.push(included_validity);
        
    }
    let mut results = vec![];
    for handle in included_handles {
        results.push(handle.await); // Joins one by one
    }
    //println!("Results for {}: {:?}", item.payload.as_ref().unwrap().grid, results);
    for result in results.iter(){
        match result {
            Ok(Some(val)) => {
                //println!("Got a string: {}", val);
                match val.as_str(){
                    "CHAIN" => {
                        //println!("Contains confirmed inclusion!");
                    }
                    _ => {
                        return None;
                    }
                }
            },
            Ok(None) => {
                return None;
            },
            Err(e) => println!("Included validation error: {}", e),
        } 
    }
    return Some(String::from("YES"));
}

pub async fn verify_signature( signature: Resource) -> bool{
    let mut decided = false;
    let mut result = false;

    let signing_key = signature.get_text_field("key");
    let mut key_match: Option<Resource> = None;
    for key in &signature.signatures{
        if key.payload.clone().unwrap().clone().grid == signing_key{
            key_match = Some(key.clone());
            break;
        }
    }
    if key_match.is_none() == true{
        //println!("Key not included...");
        return false;
    }
    let signature_validity = signatures::check_signature(signature, key_match.unwrap());
    //println!("{}", signature_validity);
    return signature_validity;
}

pub fn verify_fields(item: &Resource, ruleset: &rules::Rules) -> bool{
    //println!("Verifying fields...");
    for field in ruleset.fields{
        //println!("Checking field {} for {}", field.name, item.clone().payload.unwrap().grid);
        for rule in field.rules{
            if !verify_field(&item, field, rule){
                return false
            }
        }
    }
    return true;
}

pub fn verify_field(item: &Resource, field: &rules::Field, rule: &[&str]) -> bool{
    //println!("Checking rule: {:?}", rule);
    match rule[0]{
        "INCLUDE" => {
            match rule[1]{
                "PARENT" => {
                    for signature in &item.signatures{
                        if signature.get_entity() == item.get_entity() && signature.payload.clone().unwrap().r#type == rule[2]{
                            //println!("FIELD VALDDDIOD");
                        }
                    }
                }
                _ =>{}
            }
        }
        _ => {

        }
    }
    return true;
}