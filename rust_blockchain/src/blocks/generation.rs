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
use crate::resources::structures;

impl blocks::Header{
    pub async fn create_header(resources: &Vec::<Resource>, identifier: &Vec::<u8>) -> Option<blocks::Header>{
        //println!("Creating header with {} resources", resources.len());
        let mut new_session = state::StateSession::start_session().await;
        let mut expanded_resources: Vec::<Resource> = blocks::Header::state_mutate(resources, &mut new_session).await;
        let mut nodes = blocks::Header::generate_merkle(&expanded_resources, &mut new_session);
        let generated_state = new_session.get_root().await;
        let bloom_filter = blocks::Header::generate_bloom(&expanded_resources);
        let mut new_header = blocks::Header{
            num: 0,
            merkle_root: nodes.last_mut().unwrap()[0].hash.clone(),
            bloom: Some(bloom_filter),
            db_conn:None,
            resources_total: expanded_resources.len() as i32, 
            previous_hash: Some(utils::get_hash(identifier.to_vec())),
            resources: expanded_resources.clone(),
            merkle_tree: nodes.clone(),
            key: "".to_string(),
            signature: Vec::new(),
            state_root: generated_state,
            state: Some(new_session),

            grind_nonce: None
        };

        return Some(new_header);
    }


    pub async fn generate_header(identifier: &Vec::<u8>) -> blocks::Header{
        let mut commit_global_list = resources::ResourcePool.write().await;
        commit_global_list.push(
            Resource::create_reward(crate::get_geid(), 1000, "REWARD").await
        );
        let generated = blocks::Header::create_header(&commit_global_list, &identifier).await;
        if let Some(header) = generated{
            return header;
        }
        panic!("Error: header coulnd't be generated!");
        
    }

    pub async fn state_generate() -> Vec::<u8>{
        let mut state_session = state::StateSession::start_session().await;
        return state_session.get_root().await;
    }

    pub async fn state_mutate(commit_list: &Vec::<Resource>, state: &mut state::StateSession) -> Vec::<Resource>{
        let mut generated_count: usize  = 0;
        let mut return_list: Vec::<Resource> = Vec::new();
        for resource in commit_list.iter(){
            if resource.payload.as_ref().unwrap().action.as_str() == "GENERATED"{
                continue;
            }
            let validation = resource.verify_resource_session(state).await;
            match resource.payload.as_ref().unwrap().r#type{
                structures::ResourceCodes::Call => {
                    if let Some(mut execution) = vm::Execution::load_call(&resource).await{
                        execution.execute(state).await;
                        if !execution.success{
                            continue;
                        }
                        for (generated_grid, generated) in execution.resources{
                            if generated.mutable{
                                return_list.push(generated.resource.clone());
                                generated_count = generated_count+1;
                            }
                        }
                    }
                    else{
                        println!("Execution error!");
                    }
                }
                structures::ResourceCodes::Transaction => {
                    println!("Processed transaction!");
                    blocks::Header::process_transaction(&resource, state).await;
                }
                _ => {

                }
            }
            return_list.push(resource.clone());
            generated_count = generated_count+1;
        }
        return return_list;
    }
}