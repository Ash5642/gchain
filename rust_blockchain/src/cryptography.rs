use rsa::{Oaep, RsaPrivateKey, RsaPublicKey, Pkcs1v15Encrypt};
use rsa::pkcs8::DecodePrivateKey; 
use rsa::pkcs8::DecodePublicKey; 
use futures::stream::StreamExt;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, tls, yamux, Multiaddr, PeerId,
};
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

use rsa::signature::SignatureEncoding;


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


pub fn check_signature(mut key: EntityKey) {
    let bits = 2048;
    let mut rng = rsa::rand_core::OsRng; 

    // 2. Derive the public key from the private key

    // 3. Encrypt data using OAEP with SHA-256
    let data = b"Secret message to encrypt";
    let padding = Oaep::new::<Sha256>();
    
    let encrypted_data = key.public_key.unwrap()
        .encrypt(&mut rng, padding, &data[..])
        .expect("Encryption failed");

    ////////////////////println!("Encrypted bytes: {:?}", encrypted_data);

    // 4. Decrypt data using the private key
    let padding = Oaep::new::<Sha256>();
    let decrypted_data = key.private_key.unwrap()
        .decrypt(padding, &encrypted_data)
        .expect("Decryption failed");

    // 5. Convert bytes back to a string
    let decrypted_string = String::from_utf8(decrypted_data)
        .expect("Invalid UTF-8 sequence");

    ////////////////////println!("Decrypted message: {}", decrypted_string);

    return;
    

}

pub fn verify_pss_signature2(
    public_key: EntityKey, 
    message: &[u8], 
    signature_bytes: &[u8]
) -> Result<bool, rsa::errors::Error> {

    
    // PSS requires creating a verifying key bound to your chosen digest
    let verifying_key: VerifyingKey<Sha256> = VerifyingKey::new(public_key.public_key.unwrap());
    let signature = signature_bytes.try_into()
        .map_err(|_| rsa::errors::Error::Internal);

    match verifying_key.verify(message, &signature.unwrap()) {
        Ok(()) => {
            //////////////println!("Signature matches!");
            Ok(true)
        },
        Err(e) => {
            //////////////println!("Verification failed because: {}", e);
            Ok(false)
        }
    }
}


pub fn verify_pss_signature(
    public_key: EntityKey, 
    message: &[u8], 
    signature_bytes: &[u8]
) -> bool {
    



    let inner_public_key = public_key.public_key.unwrap();
    let encoded_sig = hex::encode(&signature_bytes);
    ////////println!("INCLUDED SIGNATURE: {}", encoded_sig);
    // 2. Instantiate the VerifyingKey with exactly 0 salt length
    let verifying_key: VerifyingKey<Sha256> = VerifyingKey::new_with_salt_len(
        inner_public_key, 
        0 // <-- Explicitly matches Python's salt_length=0
    );

    // 3. Parse the raw signature bytes safely
    let signature = match signature_bytes.try_into() {
        Ok(sig) => sig,
        Err(_) => return false,
    };

    // 4. Compute the SHA-256 hash digest of the raw protobuf bytes
    let mut hasher = Sha256::new();
    hasher.update(message);
    let hashed_digest = hasher.finalize();

    // 5. Execute the verification sequence
    verifying_key.verify(&hashed_digest, &signature).is_ok()
}

pub fn verify_ml_signature(
    public_key: EntityKeyML, 
    message: &[u8], 
    signature_bytes: &[u8]
) -> bool {
    



        let inner_public_key = public_key.public_key.unwrap();
        let encoded_sig = hex::encode(&message);
        //////println!("Protobuf: {}", encoded_sig);
        
        let mut hasher = Sha256::new();
        hasher.update(message);
        let hashed_digest = hasher.finalize();

        let signature = MLSignature::<MlDsa65>::try_from(signature_bytes)
            .map_err(|_| "Invalid signature byte length").unwrap();
        // 5. Execute the verification sequence
        let context_str: &[u8] = b""; 

        let result = inner_public_key.verify_with_context(message, context_str, &signature);
        //////println!("{}", result);
        return result
}


pub fn sign_message(
    public_key: EntityKey, 
    message: &[u8], 
) -> Result<Vec<u8>, rsa::signature::Error> {
    // 1. Initialize a mutation-free cryptosystem signing key bound to SHA-256
    let signing_key = SigningKey::<Sha256>::new(public_key.private_key.unwrap());
    let encoded = hex::encode(message);
    // 2. Initialize an OS-backed secure random number generator thread
    let bits = 2048;
    let mut rng = rsa::rand_core::OsRng; 

    // 2. Derive the public key from the private key

    // 3. Encrypt data using OAEP with SHA-256
    
    
    // 3. Generate the signature using the RandomSizedSigner trait
    // This signs the hash of your message securely
    let signature = signing_key.try_sign_with_rng(&mut rng, message)?;
    // 4. Convert the Signature struct into standard transferable raw bytes
    Ok(signature.to_bytes().into_vec())
}


pub fn load_key(mut item: &Resource) -> Option<EntityKey>{
    let payload = item.clone().payload.unwrap();


    let mut public_key:Option<RsaPublicKey> = None;
    let mut private_key:Option<RsaPrivateKey> = None;

    if payload.r#type.as_str() == "KEY"{
        for field in (*item).clone().payload.unwrap().fields{
            if field.key.as_str() == "public_key"{

                public_key = Some(RsaPublicKey::from_public_key_der(&field.value)
                    .expect("Failed to parse PKCS#1 public key bytes"));
            }
            if field.key.as_str() == "private_key"{
                private_key = Some(RsaPrivateKey::from_pkcs8_der(&field.value)
                    .expect("Failed to parse PKCS#1 public key bytes"));

            }
        }
    }

    let mut key:EntityKey = EntityKey{
        public_key:public_key,
        private_key:private_key,
        r#type: "ENTITY".to_string()
    };
    check_signature(key.clone());
    return Some(key)
}

pub fn load_ml_key(mut item: &Resource) -> Option<EntityKeyML>{
    let payload = item.clone().payload.unwrap();


    let mut public_key:Option<MLVerifyingKey::<MlDsa65>> = None;
    let mut private_key:Option<MLSigningKey::<MlDsa65>> = None;

    if payload.r#type.as_str() == "KEY"{
        for field in (*item).clone().payload.unwrap().fields{
            if field.key.as_str() == "private_key"{
                private_key = Some(MLSigningKey::<MlDsa65>::from_pkcs8_der(&field.value).ok()?);

            }
            if field.key.as_str() == "public_key"{
                public_key =  Some(MLVerifyingKey::<MlDsa65>::from_public_key_der(&field.value).ok()?);
                //////println!("Public key loaded");
            }
        }
    }

    let mut key:EntityKeyML = EntityKeyML{
        public_key:public_key,
        private_key:private_key,
        r#type: "ENTITY".to_string()
    };
    return Some(key)
}

