use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::sync::{Mutex, OnceLock};
use tokio::task;
use crate::mempool::proto_bufs::ByteMessage as ByteMessage;
use crate::mempool::proto_bufs::InternalByteMessage as InternalByteMessage;

use crate::mempool::proto_bufs::NewDocumentPayload as NewDocumentPayload;

use crate::mempool::proto_bufs::MessagePayload as MessagePayload;
use crate::mempool::MEMPOOL as MEMPOOL;
use crate::mempool::DOCUMENT_POOL as DOCUMENT_POOL;

use crate::validations::verify_transaction as verify_transaction;
use crate::validations::verify_blocks as verify_blocks;
use crate::merkle::build_merkle_tree as build_merkle_tree;
use crate::mongo_actions::add_to_ledger as add_to_ledger;

pub fn handle_message(mut hex_message:String){
    let mut buffer = [0; 512];
    let byte_message = hex::decode(&*hex_message).expect("Decoding failed");
    let message = ByteMessage::decode(&byte_message[..]).unwrap();

    return
}


pub fn handle_api_message(mut hex_message:String){
    let mut buffer = [0; 512];
    let byte_message = hex::decode(&*hex_message).expect("Decoding failed");
    let message = InternalByteMessage::decode(&byte_message[..]).unwrap();

    if message.message_type == "DOCUMENT:ADD"{
        ////////////////println!("Adding");
        let byte_document_message = hex::decode(&*message.payload).expect("Decoding failed");
        let document_message = NewDocumentPayload::decode(&byte_document_message[..]).unwrap();
        ////////////////println!("{}",document_message.grid);
        ////////////////println!("{}",document_message.hash);
        let mut list = DOCUMENT_POOL.lock().unwrap();
        list.push(document_message)
    }
    if message.message_type == "DOCUMENT:COMMIT"{
        ////////////////println!("Commiting");
        build_merkle_tree(message.payload)
    }

    return
}
