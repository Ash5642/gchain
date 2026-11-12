use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::sync::{Mutex, OnceLock};
use tokio::task;
mod block_db;
mod p2p;
mod crypt;
mod cash;
mod bloom;
mod consensus;
mod utils;
mod headers;
mod resources;
mod mempool;
mod cryptography;
mod client;
mod rules;
mod signatures;
mod entity;
mod cache;
mod self_node;
mod validators;
mod state;
mod blocks;
use mempool::MEMPOOL as MEMPOOL;
use mempool::PROCESS_QUEUE as PROCESS_QUEUE;
use mempool::PROCESS as PROCESS;
use tokio::sync::mpsc;
use mempool::BROADCAST_POOL as BROADCAST_POOL;
use p2p::start_p2p as start_p2p;
use std::env;
use proto_bufs::Resource as Resource;
use proto_bufs::ChainMessage as ChainMessage;
use proto_bufs::HostMessage as HostMessage;
mod vm;
use tokio::sync::mpsc::{Sender, Receiver};
use proto_bufs::Signature as Signature;
mod mempool_actions;
use std::sync::Arc;
use crate::resources::ChainResource;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use futures_util::StreamExt;
use tokio::time::{timeout, sleep, Duration};
static SELF_GEID: OnceLock<String> = OnceLock::new();

fn get_geid() -> &'static str {
    let args: Vec<String> = env::args().collect();
    let self_geid = &args[3];
    SELF_GEID.get_or_init(|| {
        // Safe, synchronous initialization
        format!("{self_geid}")
    })
}


pub use sha2::{Sha256, Digest};
pub mod proto_bufs {
    include!(concat!(env!("OUT_DIR"), "/proto_bufs.rs"));
}

async fn handle_client(stream: TcpStream, tx_prc: Sender<PROCESS>) {
    let (mut read_half, mut write_half) = stream.into_split();
    
    // LengthDelimitedCodec automatically reads the length prefix 
    // and spits out a complete frame of bytes for you
    let mut reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
    
    loop {
        // Apply the 60-second timeout to waiting for the next complete frame
        match timeout(Duration::from_secs(60), reader.next()).await {
            Ok(Some(Ok(bytes))) => {
                // 'bytes' is a BytesMut containing EXACTLY one full message
                match HostMessage::decode(&bytes[..]) {
                    Ok(deserialized_message) => {
                        println!("Decoded!");
                        // Success! Safely forward or process your message here
                        let cloned_deserialized_message = deserialized_message.clone();
                        let cloned_chain_message = ChainMessage{
                            r#type: deserialized_message.r#type,
                            message: deserialized_message.message
                        };
                        let Process = PROCESS{
                            node: None,
                            message: cloned_chain_message
                        };
                        match Process.message.r#type.as_str(){
                            "REQUEST"=>{
                                    println!("Got request");
                                    let process_clone = Process.clone();
                                    let message_string = &process_clone.message.message; 

                                    // 2. Pass the cloned String directly into the task (No need for as_bytes conversion here)
                                    let grid_clone = message_string.clone();
                                    //println!("Findind request");
                                    let grid_result = Resource::find_resource(String::from_utf8_lossy(&grid_clone).into_owned());
                                    //println!("Returning request");
                                    let mut host_message = proto_bufs::HostMessage{
                                        r#type: String::from("RESOURCE"),
                                        success:true,
                                        message: Vec::new()
                                    };
                                    let grid_final = grid_result.await;
                                    if grid_final.is_none() == false{
                                        //println!("Found {:?}", grid_final);
                                        cache::Cache::add_cache_db(String::from_utf8_lossy(&grid_clone).into_owned()).await;
                                        let mut resource_buf = Vec::<u8>::new();
                                        let mut buf = Vec::<u8>::new();
                                        grid_final.unwrap().encode(&mut resource_buf).unwrap();
                                        host_message.message = resource_buf.clone();
                                        host_message.encode(&mut buf).unwrap();
                                        let encoded = hex::encode(&buf);
                                        task::spawn(async move {
                                            send_length_prefixed(&mut write_half, encoded.as_str()).await;
                                        });
                                        break;
                                    }
                                    else{
                                        task::spawn(async move {
                                            send_length_prefixed(&mut write_half, "").await;
                                        });
                                        break;
                                    }
                                        
                                    
                                
                            },

                            "HOST:REQUEST"=>{
                                //println!("Request from hoist");
                                let request_protobuf = proto_bufs::ResourceRequest::decode(&Process.message.message[..]).unwrap();
                                let response_protobuf = client::Client::get_resources(request_protobuf).await;
                                let mut buf = Vec::<u8>::new();
                                response_protobuf.encode(&mut buf).unwrap();
                                let encoded = hex::encode(&buf);
                                //println!("SENDING: {}", encoded);
                                task::spawn(async move {
                                    send_length_prefixed(&mut write_half, encoded.as_str()).await;
                                });
                                break;
                            },
                            "HOST:REVOKE"=>{
                                //println!("Request from hoist");

                                let mut buf = Vec::<u8>::new();
                                let encoded = hex::encode(&buf);
                                if tx_prc.send(Process).await.is_err() {
                                    break;
                                }
                                task::spawn(async move {
                                    send_length_prefixed(&mut write_half, encoded.as_str()).await;
                                });
                                break;
                            },
                            "HOST:CONFIRMED"=>{
                                let request_protobuf = proto_bufs::ResourceRequest::decode(&Process.message.message[..]).unwrap();
                                let response_protobuf = client::Client::get_confirmed(request_protobuf).await;
                                let mut buf = Vec::<u8>::new();
                                response_protobuf.encode(&mut buf).unwrap();
                                let encoded = hex::encode(&buf);
                                task::spawn(async move {
                                    send_length_prefixed(&mut write_half, encoded.as_str()).await;
                                });
                                break;
                            },
            
                            "STATUS"=>{
                                //println!("STATUS");


                                let duration = Duration::from_secs(1);
                                let mut resource_count = 0;
                                let mut block_count = 0;
                                match timeout(duration, resources::ResourcePool.read()).await {
                                    Ok(lock) => {
                                        //println!("Pooled resources: {}", lock.len());
                                        resource_count = lock.len();

                                    },
                                    Err(_) => println!("Operation timed out after specified period!"),
                                }
                                match timeout(duration, headers::HeadersList.read()).await {
                                    Ok(lock) => {
                                        //println!("Block count: {}", lock.len());
                                        block_count = lock.len();
                                        for block in lock.iter(){
                                            let header_encoded = hex::encode(&utils::get_hash(block.to_bytes()));
                                            let previous_encoded = hex::encode(&(block.previous_hash.clone().unwrap()));
                                            //println!("Block (size: {}): {}\nPrevious: {}", block.resources_total, header_encoded, previous_encoded);
                                        }

                                    },
                                    Err(_) => println!("Operation timed out after specified period!"),
                                }
                                let response = format!("Resources: {resource_count} and Blocks:{block_count}");
                                let mut buf = Vec::<u8>::new();
                                let mut host_message = proto_bufs::HostMessage{
                                    r#type: String::from("STATUS"),
                                    success:true,
                                    message: response.into_bytes()
                                };
                                host_message.encode(&mut buf).unwrap();
                                let encoded = hex::encode(&buf);
                                task::spawn(async move {
                                    send_length_prefixed(&mut write_half, encoded.as_str()).await;
                                });
                                break;
                            }
                            _ =>{
                                if tx_prc.send(Process).await.is_err() {
                                    ////////////////println!("Consumer loop shut down. Exiting producer loop.");
                                    break;
                                }
                                task::spawn(async move {
                                    send_length_prefixed(&mut write_half, "test").await;
                                });
                                
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to decode message despite full frame: {:?}", e);
                        break; // Corrupted data structure, close connection
                    }
                }
            }
            Ok(Some(Err(e))) => {
                eprintln!("Network read error: {:?}", e);
                break;
            }
            Ok(None) => {
                println!("Client cleanly disconnected.");
                break;
            }
            Err(_timeout) => {
                println!("Client timed out after 60 seconds.");
                break;
            }
            _ => {}
        }
    }
}


pub async fn send_length_prefixed(socket: &mut tokio::net::tcp::OwnedWriteHalf, message: &str) -> std::io::Result<()> {
    let bytes = message.as_bytes();
    let len = bytes.len() as u32; // Use a fixed-size integer size

    // 1. Send the length prefix (Big-Endian network byte order)
    socket.write_u32(len).await?;
    let encoded = hex::encode(&bytes);
    //////println!("RAw daa: {}", encoded);
    // 2. Send the actual payload data
    socket.write_all(bytes).await?;
    socket.flush().await?;
    //////println!("SENDING DATA");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let self_geid = &args[3];
    SELF_GEID.get_or_init(|| {
        // Safe, synchronous initialization
        format!("{self_geid}")
    });
    block_db::load_blocks().await;
    cache::Cache::load_cache().await;
    let signing_key = self_node::sign_key().await;
    let port_num = &args[1];

    

    let addr = format!("127.0.0.1:{}", port_num);
    let listener = TcpListener::bind(addr).await?;
    ////////////////println!("Async Server listening on port {}", port_num);

    let (tx_msg, mut rx_msg) = mpsc::channel(100);
    let (tx_prc, mut rx_prc) = mpsc::channel(100);
    state::get_db().await;
    state::initialize_state().await;

    let mut new_session = state::StateSession::start_session().await;
    println!("Session root: {}", hex::encode(new_session.get_root()));
    let mut new_vm = vm::Execution::start();
    new_vm.execute(&mut new_session).await;
    println!("rand: {:?}", new_session.rand());
    println!("rand: {:?}", new_session.rand());
    println!("rand: {:?}", new_session.rand());
    //new_session.commit().await;
    //new_session.get_geid(&crate::get_geid().to_string(), Some(100 as u64)).await;
    //new_session.commit().await;
    //state::read_record(&crate::get_geid().to_string().to_string()).await;
    state::read_record(&crate::get_geid().to_string().to_string()).await;


    println!("Second tine....");

    let tx_msg_1 = tx_msg.clone();
    task::spawn(async move {
        p2p::start_processing(rx_prc, tx_msg_1).await;
    });
    let tx_prc_1 = tx_prc.clone();

    task::spawn(async move {
        start_p2p(rx_msg, tx_prc_1).await;
    });
    
    ////////////////println!("P2P server started");
    

    loop {
        let (stream, addr) = listener.accept().await?;
        let tx_prc_local = tx_prc.clone();
        tokio::spawn(async move {
            handle_client(stream, tx_prc_local).await;
            
        });
    }
}
