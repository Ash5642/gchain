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
use crate::resources::structures;


impl blocks::Header{
    pub fn generate_bloom(commit_list: &Vec::<Resource>) -> BloomFilter{
        let _my_seed = 12345u128; 
        let _base: i32 = 2;
        let mut filter = BloomFilter::with_false_pos(0.01)
            .seed(&12345u128)
            .expected_items(commit_list.len()); // Correctly constructs the BloomFilter<DefaultHasher>
        for resource_cloned in commit_list.iter(){
            let mut revoke_str: String;
            if resource_cloned.payload.as_ref().unwrap().r#type == structures::ResourceTypes::Revoke.id{
                let revoked_grid_string = resource_cloned.get_text_field("grid");
                let revoked_grids: Vec::<&str> = revoked_grid_string.split(";").collect();
                for revoked_grid in revoked_grids{
                    revoke_str = format!("-{}", revoked_grid);
                    filter.insert(&revoke_str);
                }
            }
            revoke_str = format!("+{}", resource_cloned.payload.as_ref().unwrap().grid);
            filter.insert(&revoke_str);
            
        }
        filter
    }
}