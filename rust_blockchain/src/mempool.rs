use std::sync::{Mutex, LazyLock};
use tokio::sync::RwLock;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::ChainMessage as ChainMessage;
use crate::proto_bufs::Header as HeaderProtobuf;
use crate::proto_bufs::BloomFilter as BloomFilterProtobuf;
use prost::Message;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
// Declare the global variable wrapped in a Mutex and LazyLock


pub static MEMPOOL: LazyLock<RwLock<Vec<Resource>>> = LazyLock::new(|| {
    RwLock::new(Vec::new()) // Initializes an empty vector
});

pub static BROADCAST_POOL: LazyLock<Mutex<Vec<Resource>>> = LazyLock::new(|| {
    let mut v = Vec::new();
    Mutex::new(v)
});

pub static MESSAGE_POOL: LazyLock<Mutex<Vec<ChainMessage>>> = LazyLock::new(|| {
    let mut v = Vec::new();
    Mutex::new(v)
});

pub static PROCESS_POOL: LazyLock<Mutex<Vec<ChainMessage>>> = LazyLock::new(|| {
    let mut v = Vec::new();
    Mutex::new(v)
});


pub static DOCUMENT_POOL: LazyLock<Mutex<Vec<Resource>>> = LazyLock::new(|| {
    let mut v = Vec::new();
    Mutex::new(v)
});


#[derive(Debug)]
#[derive(Clone)]

pub struct PROCESS{
    pub message:ChainMessage,
    pub node: Option<PeerId>
}


pub static PROCESS_QUEUE: LazyLock<RwLock<Vec<PROCESS>>> = LazyLock::new(|| {
    RwLock::new(Vec::new()) // Initializes an empty vector
});

pub static MESSAGE_QUEUE: LazyLock<RwLock<Vec<ChainMessage>>> = LazyLock::new(|| {
    RwLock::new(Vec::new()) // Initializes an empty vector
});

#[derive(Debug)]
#[derive(Clone)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
pub struct COMMIT{
    pub hash:Vec::<u8>,
    pub node: Option<PeerId>,
    pub value: Option<Vec::<u8>>,
    pub vote: Option<Vec::<u8>>
}

pub static COMMIT_LIST: LazyLock<RwLock<Vec<COMMIT>>> = LazyLock::new(|| {
    RwLock::new(Vec::new()) // Initializes an empty vector
});

#[derive(Debug)]
#[derive(Clone)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
pub struct BLOOMFILTER{
    pub bits: Vec::<u64>,
    pub num_hashes: u32,
}

#[derive(Debug)]
#[derive(Clone)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
pub struct BLOCKHEAD{
    pub bloom: BLOOMFILTER,
    pub merkle: Vec::<u8>,
}
#[derive(Debug)] 
#[derive(Clone)]

pub struct DBHead{
    pub merkle_root: Vec::<u8>,
}

pub static BLOCK_LIST: LazyLock<RwLock<Vec<DBHead>>> = LazyLock::new(|| {
    RwLock::new(Vec::new()) // Initializes an empty vector
}); 

pub static HEAD_LIST: LazyLock<RwLock<Vec<BLOCKHEAD>>> = LazyLock::new(|| {
    RwLock::new(Vec::new()) // Initializes an empty vector
}); 



impl BLOCKHEAD{
    pub fn to_protobuf(self) -> Vec::<u8>{
        let new_header_protobuf = HeaderProtobuf{
            num:2,
            stake: String::from(""),
            previous: Vec::new(),
            merkle_root: self.merkle,
            key: String::from(""),
            state_root: Vec::new(),
            bloom:
                Some(
                    BloomFilterProtobuf{
                        filter: self.bloom.bits
                            .iter()
                            .flat_map(|&x| x.to_ne_bytes()) // Use to_be_bytes() or to_le_bytes() for specific endianness
                            .collect(),
                        num_hashes: self.bloom.num_hashes as i32
                    }
                )
        };

        let mut buf = Vec::<u8>::new();
        new_header_protobuf.encode(&mut buf).unwrap();
        let buf_encoded = hex::encode(&buf);
        //println!("Header: {}", buf_encoded);
        return buf
    }
}