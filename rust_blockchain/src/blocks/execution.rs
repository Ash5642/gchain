use tokio_rusqlite::{params, Connection, Result};
use std::collections::HashSet;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs;
use std::fs;

use tokio::sync::mpsc;
use super::blocks;
use crate::state;
use crate::resources;
use crate::utils;
use crate::entity;
use tokio::io::AsyncWriteExt;
use fastbloom::BloomFilter;
use sha2::{Sha256, Digest};
use prost::Message; 
use crate::vm;
use crate::resources::validators;

impl blocks::Header{
    
    pub async fn process_transaction(resource: &Resource, state: &mut state::StateSession){
        let reciever = resource.get_text_field("to");
        let amt = resource.get_int_field("amt");
        let mut existing_balance: u64 = 0;
        if let Some(balance) = state.read_record(&reciever).await{
            existing_balance = balance.balance;
        }
        println!("Balance {} -> {}", existing_balance, amt);
        let new_balance = existing_balance + amt as u64;
        state.get_geid(&reciever, Some(new_balance)).await;
    }
    

}