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
use crate::blocks;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;
use crate::resources::signatures;
use std::num::TryFromIntError;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};



impl Election{
    pub async fn commit(index: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>) {
        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return, // Safely handle out-of-bounds index
        };
        if election.stage > 0 {
            return;
        }        

        let mut staked: u64 = 100;
        let mut value = vec![0u8; 32];
        let mut nonce = vec![0u8; 32];
        rand::rng().fill_bytes(&mut value);
        rand::rng().fill_bytes(&mut nonce);

        let mut combined = Vec::with_capacity(value.len() + nonce.len());
        combined.extend_from_slice(&value);
        combined.extend_from_slice(&nonce);

        println!("Committed: {}", hex::encode(&combined));

        let result = utils::get_ref_hash(&combined);
        let mut hash_stake_combined = Vec::with_capacity(value.len() + nonce.len());
        hash_stake_combined.extend_from_slice(&result);
        hash_stake_combined.extend_from_slice(&staked.to_le_bytes());


        election.nodes.insert(
            crate::get_geid().to_string(),
            Node{
                proposed: None,
                commit: result.clone().try_into().expect("IMMP"),
                commit_nonce: Some(nonce.try_into().expect("IMMP")),
                value: Some(value.clone().try_into().expect("IMMP")),
                stake: 100,
                reveal_nonce: None,
                node: None,
                geid: crate::get_geid().to_string(),
            }
        );

        Election::send_consensus_message(index, "COMMIT", &hash_stake_combined, tx_msg).await;
        election.stage = 1;
    }


    pub async fn add_commit(index: &Vec::<u8>, tx_msg: mpsc::Sender<proto_bufs::ChainMessage>, sender: PeerId, consensus_message: proto_bufs::ConsensusPayload) -> bool {
        let commit_stake = u64::from_le_bytes(consensus_message.message[32..].try_into().expect("Stake value encding error!"));


        
        println!("Commit for {} is valid staking {}", consensus_message.geid, commit_stake);

        let mut election_list = ElectionList.write().await;
        let election = match election_list.get_mut(index) {
            Some(e) => e,
            None => return false
        };
        election.staked += commit_stake;
        election.nodes.insert(
            consensus_message.geid.clone(),
            Node{
                proposed: None,
                commit: consensus_message.message[..32].try_into().expect(""),
                commit_nonce: None,
                stake: commit_stake,
                value: None,
                reveal_nonce: None,
                geid: consensus_message.geid.clone(),
                node: Some(sender)

            }
        );
        return false;
    }
}