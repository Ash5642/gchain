use rsa::{Oaep, RsaPrivateKey, RsaPublicKey, Pkcs1v15Encrypt};
use rsa::pkcs8::DecodePrivateKey; 
use rsa::pkcs8::DecodePublicKey; 
use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
use ml_dsa::signature::SignatureEncoding;
use rsa::traits::PublicKeyParts; 
use rsa::signature::{Signer, RandomizedSigner}; // Essential traits for signing
use crate::mempool_actions;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Mutex, OnceLock};
use tokio::task;
use rand::prelude;
use rand::Rng;
use std::error::Error;
use std::time::Duration;
use crate::mempool::MEMPOOL as MEMPOOL;
use crate::mempool::MESSAGE_POOL as MESSAGE_POOL;
use crate::mempool::PROCESS_POOL as PROCESS_POOL;
use crate::mempool::PROCESS_QUEUE as PROCESS_QUEUE;
use crate::mempool::PROCESS as PROCESS;
use crate::mempool;
use tokio::sync::mpsc::{Sender, Receiver};
use hex;
use crate::mempool::BROADCAST_POOL as BROADCAST_POOL;
use crate::mempool::MESSAGE_QUEUE as MESSAGE_QUEUE;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::ChainMessage as ChainMessage;
use crate::crypt;
use prost::Message; 
use crate::block_db;
use crate::bloom;
use crate::validators;
use crate::utils;

use rsa::sha2::{Sha256, Digest};
use rsa::signature::DigestVerifier;



use rsa::pss::VerifyingKey;
// 1. DO NOT USE DigestVerifier on raw bytes. Use the underlying signature verifier correctly:
use rsa::signature::Verifier; 
use rsa::pss::SigningKey;
use ml_dsa::{MlDsa65};
use ml_dsa::VerifyingKey as MLVerifyingKey;
use ml_dsa::Verifier as MLVerifier;
use ml_dsa::SigningKey as MLSigningKey;
use ml_dsa::Signer as MLSigner;
use ml_dsa::Signature as MLSignature;

use ml_dsa::pkcs8::DecodePrivateKey as MLDecodePrivateKey;
use ml_dsa::pkcs8::EncodePrivateKey as MLEncodePrivateKey;
use ml_dsa::pkcs8::DecodePublicKey as MLDecodePublicKey;
use ml_dsa::pkcs8::EncodePublicKey as MLEncodePublicKey;
use crate::resources::ChainResource;
use crate::resources;



#[derive(Clone)]
#[derive(Debug)]

pub struct EntityKey{
    public_key: Option<RsaPublicKey>,
    private_key: Option<RsaPrivateKey>,
    r#type:String,
}
#[derive(Clone)]
#[derive(Debug)]

pub struct EntityKeyML{
    public_key:Option<MLVerifyingKey::<MlDsa65>>,
    private_key: Option<MLSigningKey::<MlDsa65>>,
    r#type:String,
}


pub fn load_key(key: Resource) -> Option<EntityKeyML>{
    let public_key_encoded = key.get_byte_field("public_key").unwrap();
    let private_key_encoded = key.get_byte_field("private_key").unwrap();

    let public_key =  MLVerifyingKey::<MlDsa65>::from_public_key_der(&public_key_encoded).ok()?;
    let private_key =  MLSigningKey::<MlDsa65>::from_pkcs8_der(&private_key_encoded).ok()?;



    let loaded_key = EntityKeyML{
        public_key: Some(public_key),
        private_key: Some(private_key),
        r#type: "SIGN".to_string()
    };
    return Some(loaded_key)
}


pub fn check_signature(signature: Resource, key: Resource) -> bool{
    let loaded_key = load_key(key.clone());

    let signature_value = signature.get_byte_field("signature").unwrap();
    let message = signature.get_byte_field("resource_hash").unwrap();

    let signature = MLSignature::<MlDsa65>::try_from(&signature_value[..])
            .map_err(|_| "Invalid signature byte length").unwrap();
    let context_str: &[u8] = b""; 
    let inner_public_key = loaded_key.unwrap().public_key.unwrap();
    let result = inner_public_key.verify_with_context(&message, context_str, &signature);
    return result;
}


impl EntityKeyML{
    pub fn check_signature(&self, signature: Resource) -> bool{

        let signature_value = signature.get_byte_field("signature").unwrap();
        let message = signature.get_byte_field("resource_hash").unwrap();
        let signature = MLSignature::<MlDsa65>::try_from(&signature_value[..])
                .map_err(|_| "Invalid signature byte length").unwrap();
        let context_str: &[u8] = b""; 
        let inner_public_key = self.public_key.clone().unwrap();
        let result = inner_public_key.verify_with_context(&message, context_str, &signature);
        return result;
    }
    pub fn check_signature_bytes(&self, signature_value: Vec::<u8>, message: Vec::<u8>) -> bool{

        let signature = MLSignature::<MlDsa65>::try_from(&signature_value[..])
                .map_err(|_| "Invalid signature byte length").unwrap();
        let context_str: &[u8] = b""; 
        let inner_public_key = self.public_key.clone().unwrap();
        let result = inner_public_key.verify_with_context(&message, context_str, &signature);
        return result;
    }
    pub fn generate_signature(&self, message: Vec::<u8>) -> Vec::<u8>{
        let context_str: &[u8] = b""; 
        let inner_private_key = self.private_key.clone().unwrap();
        let signature = inner_private_key.sign(&message[..]);
        return signature.to_vec(); 
    }
}
