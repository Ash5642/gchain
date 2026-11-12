use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;
use crate::proto_bufs;
use crate::headers;
use crate::resources;
use crate::resources::ChainResource;
use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;
use prost::Message;
use crate::crypt::MerkleNode as MerkleNode;
use crate::mempool::BLOCKHEAD as BLOCKHEAD;
use crate::mempool::DBHead as DBHead;
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::mempool::HEAD_LIST as HEAD_LIST;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;


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
}




