use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;
use crate::proto_bufs;
use crate::blocks;
use crate::resources;
use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;
use prost::Message;
use crate::blocks::merkle::MerkleNode;


use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;

use crate::mempool::PROCESS as PROCESS;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};


pub struct Client{

}


impl Client{
    pub async fn get_resources(request: proto_bufs::ResourceRequest) -> proto_bufs::ResourceResponse{
        let mut response: Vec::<Resource> = Vec::new();
        for resource in request.grid.iter(){
            
            let result = Resource::find_resource(resource.to_string()).await;
            if result.is_none() == false{
                response.push(result.unwrap());
            }
        }
        let response_protobuf = proto_bufs::ResourceResponse{
            resources: response.clone()
        };
        return response_protobuf

    }
    pub async fn get_confirmed(request: proto_bufs::ResourceRequest) -> proto_bufs::GridResponse{
        let mut response: Vec::<proto_bufs::GridExists> = Vec::new();
        for resource in request.grid.iter(){
            let status = Resource::grid_status(resource).await;
            response.push(
                proto_bufs::GridExists{
                    grid: resource.clone(),
                    exists: false,
                    status: status
                }
            )
        }
        let response_protobuf = proto_bufs::GridResponse{
            exists: response.clone()
        };
        return response_protobuf
    }


    pub async fn respond(Process: PROCESS, write_half: &mut tokio::net::tcp::OwnedWriteHalf, tx_prc: Sender<PROCESS>) {
        match Process.message.r#type.as_str(){
            "REQUEST"=>{
                    //////println!("Got request");
                    let process_clone = Process.clone();
                    let message_string = &process_clone.message.message; 

                    // 2. Pass the cloned String directly into the task (No need for as_bytes conversion here)
                    let grid_clone = message_string.clone();
                    ////////println!("Findind request");
                    let grid_result = Resource::find_resource(String::from_utf8_lossy(&grid_clone).into_owned());
                    ////////println!("Returning request");
                    let mut host_message = proto_bufs::HostMessage{
                        r#type: String::from("RESOURCE"),
                        success:true,
                        message: Vec::new()
                    };
                    let grid_final = grid_result.await;
                    if grid_final.is_none() == false{
                        let mut resource_buf = Vec::<u8>::new();
                        let mut buf = Vec::<u8>::new();
                        grid_final.unwrap().encode(&mut resource_buf).unwrap();
                        host_message.message = resource_buf.clone();
                        host_message.encode(&mut buf).unwrap();
                        let encoded = hex::encode(&buf);
                        Client::send_length_prefixed(write_half, encoded.as_str()).await;
                        
                        return;
                    }
                    else{
                        Client::send_length_prefixed(write_half, "").await;
                        return;
                    }
                        
                    
                
            },

            "HOST:REQUEST"=>{
                ////////println!("Request from hoist");
                let request_protobuf = proto_bufs::ResourceRequest::decode(&Process.message.message[..]).unwrap();
                let response_protobuf = Client::get_resources(request_protobuf).await;
                let mut buf = Vec::<u8>::new();
                response_protobuf.encode(&mut buf).unwrap();
                let encoded = hex::encode(&buf);
                ////////println!("SENDING: {}", encoded);
                Client::send_length_prefixed(write_half, encoded.as_str()).await;
                
            },
            "HOST:REVOKE"=>{
                let mut buf = Vec::<u8>::new();
                let encoded = hex::encode(&buf);
                if tx_prc.send(Process).await.is_err() {
                    return;
                }
                Client::send_length_prefixed(write_half, encoded.as_str()).await;
            },
            "HOST:CONFIRMED"=>{
                let request_protobuf = proto_bufs::ResourceRequest::decode(&Process.message.message[..]).unwrap();
                let response_protobuf = Client::get_confirmed(request_protobuf).await;
                let mut buf = Vec::<u8>::new();
                response_protobuf.encode(&mut buf).unwrap();
                let encoded = hex::encode(&buf);
                Client::send_length_prefixed(write_half, encoded.as_str()).await;
    
                return;
            },

            _ =>{
                if tx_prc.send(Process).await.is_err() {
                    //////////////////////println!("Consumer loop shut down. Exiting producer loop.");
                    return;
                }
                Client::send_length_prefixed(write_half, "test").await;
                
                return;
            }
        }
    }
    pub async fn send_length_prefixed(socket: &mut tokio::net::tcp::OwnedWriteHalf, message: &str) -> std::io::Result<()> {
        let bytes = message.as_bytes();
        let len = bytes.len() as u32;
        socket.write_u32(len).await?;
        let encoded = hex::encode(&bytes);
        socket.write_all(bytes).await?;
        socket.flush().await?;
        Ok(())
    }

}




