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
use crate::mempool_actions;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Mutex, OnceLock};
use tokio::task;
use rand::prelude;
use rand::Rng;
use crate::cash;
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
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
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
use crate::headers;
use crate::proto_bufs::ChainMessage as ChainMessage;
use sha2::{Sha256, Digest};
use crate::crypt;
use prost::Message; 
use crate::block_db;
use crate::bloom;
use crate::validators;
use crate::state;
use crate::utils;
use crate::resources;
use crate::resources::ChainResource;
// 1. Define the custom behavior combining Gossipsub and MDNS
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "LocalNodeEvent")]
struct LocalNodeBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[derive(Clone)]
struct Vote{
    node: Option<PeerId>,
    hash: Vec::<u8>,
    reveal:Option<Vec::<u8>>
}

// 2. Define the generated event enum variants
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
    let mut commit_list = Vec::<Vote>::new();
    let mut has_voted = false;
    let mut my_val:Vec::<u8> = Vec::new();
    ////////////////println!("P2p started");
    // 3. Initialize the Swarm with TCP, TLS/Noise, and Yamux multiplexing
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            (tls::Config::new, noise::Config::new),
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            // Setup Gossipsub config
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

            // Setup MDNS for local discovery
            let mdns = mdns::tokio::Behaviour::new(
                mdns::Config::default(), 
                key.public().to_peer_id()
            )?;

            Ok(LocalNodeBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // 4. Create and subscribe to a Gossipsub topic
    let topic = gossipsub::IdentTopic::new("local-chat");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    // 5. Start listening on all local interfaces with a random OS-assigned port
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    println!("Started with node id: {:?}", swarm.local_peer_id());
    let config_data = Some((*swarm.local_peer_id()).clone());
    if SELF_ADDR.set(config_data).is_err() {
        ////////////////println!("Global state was already initialized!");
    }
    let mut broadcast_interval = tokio::time::interval(Duration::from_secs(1));
    let mut message_counter = 0;
    let mut broadcast_pos = 0;
    loop {
        
         tokio::select! {
            // 1. Trigger broadcast every 5 seconds
            _ = broadcast_interval.tick() => {                
            }
            Some(job) = rx_msg.recv() => {
                //println!("Message {} queued", job.r#type);
                let mut buf = Vec::<u8>::new();
                job.encode(&mut buf).unwrap();
                //println!("Sending broadcast of size: {}", buf.len());
                match swarm.behaviour_mut().gossipsub.publish(topic.clone(), buf) {
                    Ok(message_id) => {
                        //println!("SENT {}", job.r#type);

                    }
                    Err(gossipsub::PublishError::InsufficientPeers) => {
                        //////////////////println!("Broadcast skipped: No local peers connected yet.");
                    }
                    Err(e) => {
                        ////////////////println!("Failed to broadcast message: {:?}", e);
                    }
                }
                
            }

            // 2. Drive the swarm network events as before
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(LocalNodeEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, multiaddr) in list {
                            //println!("Discovered local peer via MDNS: {peer_id} at {multiaddr}");
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(LocalNodeEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, multiaddr) in list {
                            ////////////////////println!("Local peer expired: {peer_id} at {multiaddr}");
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    }                  
                    SwarmEvent::Behaviour(LocalNodeEvent::Gossipsub(Event::Message {
                        propagation_source,
                        message_id,
                        message,
                    })) => {
                        // Your custom check (e.g. block height, data parsing, payload limits)
                        let is_valid = true;
                        let deserialized_message = ChainMessage::decode(&message.data[..]).unwrap();
                        //println!("RECIEVED {} !!!", deserialized_message.r#type);
                        let message_encoded = hex::encode(&deserialized_message.message);
                        ////println!("{}", message_encoded);
                        let new_process = PROCESS{
                            node: message.source,
                            message: deserialized_message
                        };
                        if tx_prc.send(new_process).await.is_err() {}

                    }
                    SwarmEvent::NewListenAddr { address, .. } => {
                        ////////////////println!("Listening on global address: {address}");
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
            // 1. Trigger broadcast every 5 seconds
            
            Some(job) = rx_prc.recv() => {
                //////////////////println!("Recieved job {} from {:?}", job.message.r#type, job.node);
                let mut resp: Option<ChainMessage> = None;
                println!("Starting job {}", job.message.r#type.as_str());
                match job.message.r#type.as_str(){
                    "CMD:ELECTION"=>{
                        let new_header = headers::Header::generate_header(Vec::new()).await;
                        let head_saved = headers::Header::add_block_db(new_header.clone()).await;
                        let blocks_saved = headers::Header::save_resources(new_header.clone()).await;
                    }
                    "FAUCET"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            faucet(new_tx_msg).await;
                        });
                    }
                    "BLOCK" => {
                        let load_block = proto_bufs::NewBlock::decode(&(job.message.message.clone())[..]).unwrap();
                        let load_header = load_block.header.unwrap();
                        let merkle_encoded = hex::encode(load_header.merkle_root.clone());
                        println!("Block recieved: {}", merkle_encoded);
                        let new_header = headers::Header::add_header(
                            load_header.num as u32,
                            load_header.merkle_root.clone(),
                            load_header.bloom.as_ref().unwrap().filter
                                .chunks_exact(8)
                                .map(|chunk| {
                                    let array: [u8; 8] = chunk.try_into().unwrap();
                                    u64::from_ne_bytes(array) // Use from_be_bytes or from_le_bytes for specific endianness
                                })
                                .collect(),
                            load_header.bloom.as_ref().unwrap().num_hashes as u32,
                            0,
                            Some(Vec::new())
                        );
                        headers::Header::add_block_db(new_header.clone()).await;
                        let merkle_encoded = hex::encode(load_header.merkle_root.clone());
                        let args: Vec<String> = env::args().collect();
                        let db_dir = args[2].clone();
                        let db_name = format!("{db_dir}/{merkle_encoded}.db");
                        fs::write(db_name, load_block.resources);
                    }
                    "CMD:REVEAL"=>{
                        let e_resp = Some(
                            ChainMessage{
                                r#type: String::from("PHASE:REVEAL"),
                                message: String::from("test").into_bytes()
                            }
                        );
                        if tx_msg.send(e_resp.clone().unwrap()).await.is_err() {
                            ////////////////println!("Consumer loop shut down. Exiting producer loop.");
                        }
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            reveal_vote(new_tx_msg).await;
                        });
                    }

                    "CMD:CONFIRM"=>{
                        let e_resp = Some(
                            ChainMessage{
                                r#type: String::from("PHASE:CONFIRM"),
                                message: String::from("test").into_bytes()
                            }
                        );
                        if tx_msg.send(e_resp.clone().unwrap()).await.is_err() {
                            ////////////////println!("Consumer loop shut down. Exiting producer loop.");
                        }
                        let mut new_tx_msg = tx_msg.clone();
                    }

                    "HOST:REVOKE"=>{
                        println!("Revoking....");
                        let mut new_tx_msg = tx_msg.clone();
                        add_revoke(job.message.message, new_tx_msg).await;
                        
                    }
                    "CMD:RESOURCE"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        ////println!("ADDED");
                        task::spawn(async move {
                            add_resource(job.node, Some(job.message.message), new_tx_msg, true).await;
                        });
                    }
                    "CMD:RESOURCES"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        ////println!("ADDED");
                        task::spawn(async move {
                            add_resource(job.node, Some(job.message.message), new_tx_msg, true).await;
                        });
                    }
                    "ELECTION"=>{
                        //println!("Starting election!!");
                        let mut header_list = headers::HeadersList.read().await;
                        let header_num = header_list.len();
                        let identifier = (header_num as i32).to_le_bytes().to_vec();
                        drop(header_list);
                        //println!("Starting election {:?}!", identifier);
                        let election_index = consensus::Election::start_election(Some(identifier).clone()).await;
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            consensus::Election::commit(election_index, new_tx_msg).await
                        });
                        
                    }
                    "REVEAL"=>{
                        //println!("Starting reveal!");
                        let mut header_list = headers::HeadersList.read().await;
                        let header_num = header_list.len();
                        let identifier = (header_num as i32).to_le_bytes().to_vec();
                        drop(header_list);
                        let election_index = consensus::Election::start_election(Some(identifier)).await;
                        println!("Elindex: {}", election_index);
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            consensus::Election::reveal(election_index, new_tx_msg).await
                        });
                        
                    }
                    "TARGET"=>{
                        //println!("Starting distance!");
                        let mut header_list = headers::HeadersList.read().await;
                        let header_num = header_list.len();
                        let identifier = (header_num as i32).to_le_bytes().to_vec();
                        drop(header_list);
                        let election_index = consensus::Election::start_election(Some(identifier)).await;
                        println!("Elindex: {}", election_index);
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            consensus::Election::set_target(election_index, new_tx_msg).await
                        });
                    }
                    "SHOW"=>{
                        //println!("Showing election!");
                        let election_list = consensus::ElectionList.read().await;
                        let mut target = vec![0u8; 32];
                        rand::rng().fill_bytes(&mut target);
                        println!("Target: {}", hex::encode(target.clone()));
                        
                        for election in election_list.iter(){
                            println!("\nElection: {}\nValidator: {:?}\n", hex::encode(election.identifier.clone()), election.validator);
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
                            ////////////////println!("Consumer loop shut down. Exiting producer loop.");
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
                                println!("Recieved commit!");
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
                                println!("Checking proposed block!");
                                consensus::Election::add_validation(election_index, consensus_message.clone(), new_tx_msg).await;
                            } 

                            _ => {}
                        }

                    }
                    "PHASE:REVEAL"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            reveal_vote(new_tx_msg).await;
                        });
                    }
                    "PHASE:CONSENSUS"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        task::spawn(async move {
                            get_target(new_tx_msg).await;
                        });
                    }
                    "COMMIT"=>{
                        
                        let mut new_tx_msg = tx_msg.clone();
                        
                        task::spawn(async move {
                            add_commit(job.node, job.message.message, None).await;
                        });

                        
                    }
                    "REVEAL"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        
                        task::spawn(async move {
                            add_reveal(job.node, Some(job.message.message)).await;
                        });

                        
                    }
                    "RESOURCE"=>{
                        let mut new_tx_msg = tx_msg.clone();
                        ////println!("ADDED");
                        task::spawn(async move {
                            add_resource(job.node, Some(job.message.message), new_tx_msg, false).await;
                        });
                    }
                    _=>{
                        ////////////////println!("");
                        
                    }
                }
                if resp.as_ref().is_none() == false{
                    if tx_msg.send(resp.clone().unwrap()).await.is_err() {
                        ////////////////println!("Consumer loop shut down. Exiting producer loop.");
                        break;
                    }
                }
                    
                

            }
         }
    }
    


    
}


pub async fn generate_vote(tx_msg: Sender<ChainMessage>){
    ////////////////println!("GENERATING");
    let mut dynamic_bytes = vec![0u8; 64];
    //////////////////println!("Dynamic vector length: {}", dynamic_bytes.len());
    let mut result;
    rand::rng().fill_bytes(&mut dynamic_bytes);
    let mut hasher = Sha256::new();
    hasher.update(&dynamic_bytes);
    result = hasher.finalize().to_vec();
    let encoded_value = hex::encode(&dynamic_bytes);
    let mut chain = ChainMessage{
        r#type: String::from("COMMIT"),
        message:result.clone(),
    };
    if tx_msg.send(chain.clone()).await.is_err() {
        ////////////////println!("Consumer loop shut down. Exiting producer loop.");
    }
    task::spawn(async move {
        if let Some(config) = SELF_ADDR.get() {
            ////////////////println!("Task 1 read config: {:?}", config);
            add_commit(*config, result, Some(dynamic_bytes.clone())).await;
        }
    });
}
pub async fn add_commit(address: Option<PeerId>, hash: Vec::<u8>, value: Option<Vec::<u8>>){
    let encoded = hex::encode(&hash);
    let mut commit_list = mempool::COMMIT_LIST.write().await;
    for commit in commit_list.iter(){
        if commit.node == address{
            ////////////////println!("RDDED {:?}:{}", address, encoded);
            return
        }
    }
    commit_list.push(
        mempool::COMMIT{
            hash: hash.clone(),
            node: address,
            value: value,
            vote: None
        }
    );
    
    ////////////////println!("Added {:?}:{}", address, encoded);
    drop(commit_list)
}



pub async fn reveal_vote(tx_msg: Sender<ChainMessage>){
    let mut commit_list = mempool::COMMIT_LIST.read().await;
    for commit in commit_list.iter(){
        if let Some(config) = SELF_ADDR.get() {
            ////////////////println!("Task 1 read config: {:?}", config);
            if commit.node == *config{
                let reveal_message = ChainMessage{
                    r#type: String::from("REVEAL"),
                    message:commit.value.clone().unwrap()
                };
                if tx_msg.send(reveal_message.clone()).await.is_err() {
                    ////////////////println!("Consumer loop shut down. Exiting producer loop.");
                }
                return;
            }
        }
        
    }
    drop(commit_list)
    
}

pub async fn add_reveal(address: Option<PeerId>, value: Option<Vec::<u8>>){
    let mut commit_list = mempool::COMMIT_LIST.write().await;
    for commit in commit_list.iter_mut(){
        if commit.node == address{
            //////////////////println!("REVEAL!");
            commit.value = value.clone();
            let mut encoded = hex::encode(value.clone().unwrap());
            ////////////////println!("Revealed value for {:?}: {}", address, encoded);
            return
        }
    }
    ////////////////println!("No COMMIT found!");
    drop(commit_list)
}



pub async fn get_target(tx_msg: Sender<ChainMessage>) -> Option<mempool::COMMIT>{
    let mut target: Vec<u8> = vec![0; 32];
    let mut commit_list = mempool::COMMIT_LIST.write().await;
    for vote in commit_list.iter(){
        if vote.value.is_none() == false{
            let encoded = hex::encode(vote.value.clone().unwrap());
            ////////////////println!("For {}", encoded);
            target = xor_two_vectors(&target, &vote.value.as_ref().unwrap()[..32])  
        }
         
    }
    let mut encoded = hex::encode(&target);
    let mut hasher = Sha256::new();
    hasher.update(&target);
    target = hasher.finalize().to_vec();

    encoded = hex::encode(&target);
    ////////////////println!("Target value: {}", encoded);
    let mut min = 256;
    let mut result: Option<mempool::COMMIT> = None;
    
    for vote in commit_list.iter_mut(){
        let distance  = hamming_vector_distance(&target, &vote.value.as_ref().unwrap()[..32]);
        ////////////////println!("Distance: {}", distance); 
        if distance < min {
            result = Some((*vote).clone());
            min = distance;
        }
        
    }
    if let Some(config) = SELF_ADDR.get() {
        ////////////////println!("Task 1 read config: {:?}", config);
        if result.unwrap().node == *config{
           //println!("WINNERWINNER!!!!!!");
            headers::Header::generate_header(Vec::<u8>::new()).await;
            
            
        }
    }
    
    
    return None
}


pub async fn add_resource2(address: Option<PeerId>, value: Option<Vec::<u8>>, tx_msg: Sender<ChainMessage>, broadcast:bool){
    
    let deserialized_resource = Resource::decode(&(value.clone().unwrap())[..]).unwrap();   
    let resource_clone = deserialized_resource.clone();
    let validity = resource_clone.validate_add().await;

    
    if validity == false{
        println!("{} not added to queue", deserialized_resource.payload.clone().unwrap().grid);
        return
    }
    else{
        println!("{} added to queue", deserialized_resource.payload.clone().unwrap().grid);
        let item_validity = validators::verify_proof(deserialized_resource.clone()).await;
        if item_validity == false{
            println!("Not included!");
            return
        }
        println!("Adding! Valid");
        
        resource_clone.add_resource().await;
        let e_resp = Some(
            ChainMessage{
                r#type: String::from("RESOURCE"),
                message: value.unwrap()
            }
        );
        if broadcast{
            if tx_msg.send(e_resp.clone().unwrap()).await.is_err() {
                ////////////////println!("Consumer loop shut down. Exiting producer loop.");
            }
        }
    }
}


pub async fn add_resource(address: Option<PeerId>, value: Option<Vec::<u8>>, tx_msg: Sender<ChainMessage>, broadcast:bool){
    let deserialized_resources = proto_bufs::BulkResources::decode(&(value.clone().unwrap())[..]).unwrap(); 
    println!("Serving bulk!");  
    for resource in deserialized_resources.resources{
        println!("Serving PALKLL!");
        let resource_clone = resource.clone();
        let resource_exists_clone = resource.clone();
        let mut tx_msg_clone = tx_msg.clone();
        let check_exists = task::spawn(
            async move {
                let exists_check = resource_exists_clone.exists();
                let validation_check = validators::verify_resource(resource_exists_clone.clone(), None);
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
                println!("Resource is valid!");
                if resource_exists_clone.payload.as_ref().unwrap().r#type.as_str() == "CALL"{
                    println!("Serving call!");
                    let mut new_session = state::StateSession::start_session().await;
                    let mut new_execution = vm::Execution::load_call(resource_exists_clone.clone()).await;
                    new_execution.execute(&mut new_session).await;
                    new_session.commit().await;
                }

                resource_exists_clone.add_resource().await;
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
                if broadcast{
                    if tx_msg_clone.send(e_resp.clone().unwrap()).await.is_err() {
                        println!("Consumer loop shut down. Exiting producer loop.");
                    }
                }            
            }
        );

    }
}

pub async fn faucet(tx_msg: Sender<ChainMessage>){
    let identifier_encoded = "FAUCET";
    let geid = crate::get_geid().to_string();
    let cash = cash::Cash::faucet(1000).await;
    let resource_packaged = proto_bufs::BulkResources{
        resources: vec![cash.clone()]
    };
    let mut resource_encoded = Vec::<u8>::new();
    resource_packaged.encode(&mut resource_encoded).unwrap();
    let e_resp = Some(
        ChainMessage{
            r#type: String::from("RESOURCE"),
            message: resource_encoded
        }
    );
    if tx_msg.send(e_resp.clone().unwrap()).await.is_err() {
        println!("Consumer loop shut down. Exiting producer loop.");
    }
    cash.add_resource().await;
    println!("Faucet saved!");

}

pub async fn add_revoke(value: Vec::<u8>, tx_msg: Sender<ChainMessage>){
    let deserialized_resource = proto_bufs::ResourceRequest::decode(&(value.clone())[..]).unwrap(); 
    for revoked_resource in deserialized_resource.grid{
        let payload = Payload{
            grid: revoked_resource.clone(),
            action: String::from("REVOKE"),
            fields: Vec::new(),
            nonce: Vec::new(),
            r#type: String::from("REVOKE")
        };
        let deserialized_resource = Resource{
            payload: Some(payload),
            signatures: Vec::new(),
            proof: Vec::new(),
            status: 4
        };
        deserialized_resource.add_resource().await;
        println!("Revoked: {}", revoked_resource);
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
