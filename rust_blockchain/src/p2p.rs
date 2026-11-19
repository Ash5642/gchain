use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
use libp2p::gossipsub::{Event, MessageAcceptance};
use libp2p::gossipsub::{ConfigBuilder, ValidationMode};
use std::fs;
use std::env;
use std::io;
use crate::vm;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Mutex, OnceLock};
use tokio::task;
use rand::prelude;
use rand::Rng;

use std::fs::File;
use rocksdb;
use std::io::Write;
use std::error::Error;
use std::time::Duration;
use crate::mempool::MEMPOOL as MEMPOOL;
use crate::mempool::MESSAGE_POOL as MESSAGE_POOL;
use crate::mempool::PROCESS_POOL as PROCESS_POOL;
use crate::mempool::PROCESS_QUEUE as PROCESS_QUEUE;
use crate::mempool::PROCESS as PROCESS;

use crate::consensus;
use crate::mempool;
use crate::proto_bufs;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::blocks;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};

use prost::Message; 


use crate::resources::validators;
use crate::state;
use crate::utils;
use crate::resources;
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "LocalNodeEvent")]
struct LocalNodeBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}


enum LocalNodeEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<gossipsub::Event> for LocalNodeEvent {
    fn from(event: gossipsub::Event) -> Self {
        LocalNodeEvent::Gossipsub(event)
    }
}

impl From<mdns::Event> for LocalNodeEvent {
    fn from(event: mdns::Event) -> Self {
        LocalNodeEvent::Mdns(event)
    }
}

static SELF_ADDR: OnceLock<Option<PeerId>> = OnceLock::new();

pub async fn start_p2p(mut rx_msg: Receiver<ChainMessage>, tx_prc: Sender<PROCESS>) -> Result<(), Box<dyn Error>> {
    println!("P2p started");
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            (tls::Config::new, noise::Config::new),
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .max_transmit_size(10 * 1_048_576)
                .heartbeat_interval(Duration::from_secs(1))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .build()
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::Other, msg))?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns = mdns::tokio::Behaviour::new(
                mdns::Config::default(), 
                key.public().to_peer_id()
            )?;

            Ok(LocalNodeBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let topic = gossipsub::IdentTopic::new("local-chat");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    let config_data = Some((*swarm.local_peer_id()).clone());
    if SELF_ADDR.set(config_data).is_err() {
        println!("Global state was already initialized!");
    }
    let mut broadcast_interval = tokio::time::interval(Duration::from_secs(1));
    let mut message_counter = 0;
    let mut broadcast_pos = 0;
    loop {
         tokio::select! {
            _ = broadcast_interval.tick() => {                
            }
            Some(job) = rx_msg.recv() => {
                let mut buf = Vec::<u8>::new();
                job.encode(&mut buf).unwrap();
                match swarm.behaviour_mut().gossipsub.publish(topic.clone(), buf) {
                    Ok(message_id) => {
                    }
                    Err(gossipsub::PublishError::InsufficientPeers) => {
                    }
                    Err(e) => {
                    }
                }
                
            }

            // 2. Drive the swarm network events as before
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(LocalNodeEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, multiaddr) in list {
                            ////println!("Discovered local peer via MDNS: {peer_id} at {multiaddr}");
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(LocalNodeEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, multiaddr) in list {
                            //////////////////////println!("Local peer expired: {peer_id} at {multiaddr}");
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    }                  
                    SwarmEvent::Behaviour(LocalNodeEvent::Gossipsub(Event::Message {
                        propagation_source,
                        message_id,
                        message,
                    })) => {
                        let is_valid = true;
                        let deserialized_message = ChainMessage::decode(&message.data[..]).unwrap();
                        let message_encoded = hex::encode(&deserialized_message.message);
                        let new_process = PROCESS{
                            node: message.source,
                            message: deserialized_message
                        };
                        if tx_prc.send(new_process).await.is_err() {}

                    }
                    SwarmEvent::NewListenAddr { address, .. } => {
                        //////////////////println!("Listening on global address: {address}");
                    }
                    _ => {}
                }
            }
        }
    }
}

pub async fn start_processing(mut rx_prc: Receiver<PROCESS>, tx_msg: Sender<ChainMessage>){

    loop {
        
         tokio::select! {            
            Some(job) = rx_prc.recv() => {
                let mut resp: Option<ChainMessage> = None;
                match job.message.r#type.as_str(){
                    "CMD:ELECTION"=>{
                        //test path to skip consensus
                        let new_header = blocks::Header::generate_header(Vec::new()).await;
                        let head_saved = blocks::Header::add_block_db(&new_header).await;
                        let blocks_saved = blocks::Header::save_resources(&new_header).await;
                    }

                    "CMD:RESOURCES"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            add_resource(&job.message.message, new_tx_msg, true).await;
                        });
                    }
                    "ELECTION"=>{
                        ////println!("Starting election!!");
                        let mut header_list = blocks::HeadersList.read().await;
                        let header_num = header_list.len();
                        let identifier = (header_num as i32).to_le_bytes().to_vec();
                        drop(header_list);
                        ////println!("Starting election {:?}!", identifier);
                        let election_index = consensus::Election::start_election(Some(identifier).clone()).await;
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            consensus::Election::commit(election_index, new_tx_msg).await
                        });
                        
                    }
                    "REVEAL"=>{
                        ////println!("Starting reveal!");
                        let mut header_list = blocks::HeadersList.read().await;
                        let header_num = header_list.len();
                        let identifier = (header_num as i32).to_le_bytes().to_vec();
                        drop(header_list);
                        let election_index = consensus::Election::start_election(Some(identifier)).await;
                        //println!("Elindex: {}", election_index);
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            consensus::Election::reveal(election_index, new_tx_msg).await
                        });
                        
                    }
                    "TARGET"=>{
                        ////println!("Starting distance!");
                        let mut header_list = blocks::HeadersList.read().await;
                        let header_num = header_list.len();
                        let identifier = (header_num as i32).to_le_bytes().to_vec();
                        drop(header_list);
                        let election_index = consensus::Election::start_election(Some(identifier)).await;
                        //println!("Elindex: {}", election_index);
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            consensus::Election::set_target(election_index, new_tx_msg).await
                        });
                    }
                    "SHOW"=>{
                        ////println!("Showing election!");
                        let election_list = consensus::ElectionList.read().await;
                        let mut target = vec![0u8; 32];
                        rand::rng().fill_bytes(&mut target);
                        //println!("Target: {}", hex::encode(target.clone()));
                        
                        for election in election_list.iter(){
                            //println!("\nElection: {}\nValidator: {:?}\n", hex::encode(election.identifier.clone()), election.validator);
                            for node in election.nodes.iter(){
                                let distance = utils::xor_bit_distance(&node.value[..], &target[..]);
                                println!(
                                    "Node: {}\nCommited: {}\nValue: {}\nNonce: {}\nDistance: {}\nVote: {}\n", 
                                    node.geid, 
                                    hex::encode(node.commit.clone()),
                                    hex::encode(node.value.clone()),
                                    hex::encode(node.nonce.clone()),
                                    distance,
                                    node.validated
                                );
                            }
                        }
                    }
                    "CMD:DUMP" => {
                        utils::create_dump().await;
                        let e_resp = Some(
                            ChainMessage{
                                r#type: String::from("DUMP"),
                                message: String::from("test").into_bytes()
                            }
                        );
                        if tx_msg.send(e_resp.clone().unwrap()).await.is_err() {
                            //////////////////println!("Consumer loop shut down. Exiting producer loop.");
                        }
                    }
                    "DUMP" => {
                        utils::create_dump().await;
                    }
                    "CONSENSUS" => {
                        let consensus_message = proto_bufs::ConsensusMessage::decode(&(job.message.message)[..]).unwrap(); 
                        let election_index = consensus::Election::start_election(Some(consensus_message.identifier.clone())).await;
                        let mut new_tx_msg_commit = tx_msg.clone();
                        task::spawn(async move {
                            consensus::Election::commit(election_index, new_tx_msg_commit).await
                        });
                        let mut new_tx_msg = tx_msg.clone();
                        match consensus_message.r#type.as_str(){
                            "COMMIT" => {
                                //println!("Recieved commit!");
                                task::spawn(async move {
                                    consensus::Election::add_commit(election_index, new_tx_msg, job.node.unwrap().clone(), consensus_message.clone()).await
                                });
                            } 

                            "REVEAL" => {
                                let mut new_tx_msg_reveal = tx_msg.clone();
                                task::spawn(async move {
                                    consensus::Election::reveal(election_index, new_tx_msg_reveal).await
                                });
                                task::spawn(async move {
                                    consensus::Election::add_reveal(election_index, new_tx_msg, consensus_message.clone()).await
                                });
                            } 
                            "TARGET" => {
                                consensus::Election::add_target(election_index, consensus_message.message.clone(), new_tx_msg).await;
                                
                            } 
                            "PROPOSE" => {
                                consensus::Election::validate_block(election_index, consensus_message.clone(), new_tx_msg).await;
                            } 
                            "VALIDATE" => {
                                //println!("Checking proposed block!");
                                consensus::Election::add_validation(election_index, consensus_message.clone(), new_tx_msg).await;
                            } 

                            _ => {}
                        }

                    }
                    
                

                    "RESOURCE"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            add_resource(&job.message.message, new_tx_msg, false).await;
                        });
                    }
                    _=>{
                        println!("Unknown type recieved!");
                    }
                }
                if resp.as_ref().is_none() == false{
                    if tx_msg.send(resp.unwrap()).await.is_err() {
                        break;
                    }
                }
                    
                

            }
         }
    }
    


    
}




pub async fn add_resource(value: &Vec::<u8>, tx_msg: Sender<ChainMessage>, broadcast:bool){
    let deserialized_resources = proto_bufs::BulkResources::decode(&(value)[..]).unwrap();
    for resource in deserialized_resources.resources{
        let resource_clone = resource.clone();
        let mut tx_msg_clone = tx_msg.clone();
        let check_exists = task::spawn(
            async move {
                //println!("Recieved: {}", resource_clone.payload.as_ref().unwrap().grid);
                let exists_check = resource_clone.exists();
                let validation_check = validators::verify_resource(&resource_clone, None);
                let exists = exists_check.await;
                let validity = validation_check.await;
                if !exists.is_none(){
                    println!("Resource exists!");
                    return;
                }
                if validity.is_none(){
                    println!("Resource is invalid!");
                    return;
                }


                resource_clone.add_resource().await;
                let mut new_state = state::StateSession::start_session().await;
                
                if broadcast{
                    let resource_packaged = proto_bufs::BulkResources{
                        resources: vec![resource_clone]
                    };
                    let mut resource_encoded = Vec::<u8>::new();
                    resource_packaged.encode(&mut resource_encoded).unwrap();
                    let e_resp = Some(
                        ChainMessage{
                            r#type: String::from("RESOURCE"),
                            message: resource_encoded
                        }
                    );
                    if tx_msg_clone.send(e_resp.unwrap()).await.is_err() {}

                }            
            }
        );
        

    }
}


fn xor_two_vectors(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(&x, &y)| x ^ y).collect()
}

fn hamming_vector_distance(a: &[u8], b: &[u8]) -> u32 {
    a.iter()
     .zip(b)
     .map(|(x, y)| (x ^ y).count_ones())
     .sum()
}
