use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;
use crate::proto_bufs;
use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;
use prost::Message;
use crate::headers;
use crate::validators;
use std::fs;
use crate::crypt::MerkleNode as MerkleNode;
use crate::mempool::BLOCKHEAD as BLOCKHEAD;
use crate::mempool::DBHead as DBHead;
use crate::mempool::BLOCK_LIST as BLOCK_LIST;
use crate::mempool::HEAD_LIST as HEAD_LIST;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use crate::resources::ChainResource;
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
use crate::signatures;
use crate::cash;
use rocksdb::{DB, Options, FlushOptions, WriteOptions};
use std::sync::OnceLock;
use tokio::sync::OnceCell;
use async_recursion::async_recursion;
use crate::state;
use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

mod resolve; 
mod resource_management; 
mod memory;
mod cash_management;
mod vm_cryptography;

#[derive(Clone)]

pub struct ExecutionIO{
    key: String,
    r#type: u8,
    resource: Option<Resource>
}

#[derive(Clone)]
pub struct ResourceVar{
    resource: Resource,
    mutable: bool
}


#[derive(Clone)]

pub struct MemoryVar{
    key: Vec::<u8>,
    r#type: String,
    value: Vec::<u8>
}

#[derive(Clone)]
pub struct MemorySpace{
    vars: HashMap<Vec<u8>, MemoryVar>
}


#[derive(Clone)]

pub struct Frame{
    pub memory: MemorySpace,
    pub inputs: Vec::<ExecutionIO>,
    pub returns: Vec::<ExecutionIO>,
    pub instructions: Vec::<Instruction>
}


#[derive(Clone)]
pub struct Balance{
    pub geid: String,
    pub balance: u64
}

#[derive(Clone)]
pub struct Execution{
    executable: String,
    entity: String,
    inputs: Vec::<ExecutionIO>,
    memory: MemorySpace,
    pub generated: HashMap<String, Resource>,
    pub loaded: HashMap<String, Resource>,
    pub resources: HashMap<String, ResourceVar>,
    stack: Vec::<Frame>,
    balances: Vec::<Balance>,
    caller: String,
    cost: u64
}


impl Execution{

    pub fn start() -> Execution{
        let main_frame = Frame{
            memory: MemorySpace{
                vars: HashMap::new()
            },
            inputs: Vec::new(),
            returns: Vec::new(),
            instructions: vec![
                

                Instruction{
                    operation: "CASH".to_string(),
                    write: Some(
                        Operand{
                            r#type: "M".to_string(),
                            key: "IssuedGrid".bytes().collect(),
                            args: vec![]
                        }
                    ),
                    operands: vec![
                        Operand{
                            r#type: "E".to_string(),
                            key: "geid".bytes().collect(),
                            args: vec![]
                        },
                        Operand{
                            r#type: "D".to_string(),
                            key: u64::from(5 as u64).to_le_bytes().to_vec(),
                            args: vec![]
                        }
                    ]
                },
                Instruction{
                    operation: "LOAD".to_string(),
                    write: None,
                    operands: vec![
                        Operand{
                            r#type: "D".to_string(),
                            key: "TestUser1[KEY][SIGN]=nvdwj8IFSlpfH7MIOpcKNOm2KUoaNcew".bytes().collect(),
                            args: vec![]
                        }
                    ]
                },
                Instruction{
                    operation: "SIGN".to_string(),
                    write: Some(
                        Operand{
                            r#type: "M".to_string(),
                            key: "SIGNATURE_GEN".bytes().collect(),
                            args: vec![]
                        }
                    ),
                    operands: vec![
                        Operand{
                            r#type: "D".to_string(),
                            key: "TestUser1[KEY][SIGN]=nvdwj8IFSlpfH7MIOpcKNOm2KUoaNcew".bytes().collect(),
                            args: vec![]
                        },
                        Operand{
                            r#type: "D".to_string(),
                            key: "COCK".bytes().collect(),
                            args: vec![]
                        }
                    ]
                },
                Instruction{
                    operation: "CSIGN".to_string(),
                    write: None,
                    operands: vec![
                        Operand{
                            r#type: "D".to_string(),
                            key: "TestUser1[KEY][SIGN]=nvdwj8IFSlpfH7MIOpcKNOm2KUoaNcew".bytes().collect(),
                            args: vec![]
                        },
                        Operand{
                            r#type: "D".to_string(),
                            key: "COCK".bytes().collect(),
                            args: vec![]
                        },
                        Operand{
                            r#type: "M".to_string(),
                            key: "SIGNATURE_GEN".bytes().collect(),
                            args: vec![]
                        }
                    ]
                },
                Instruction{
                    operation: "PRINT".to_string(),
                    write: None,
                    operands: vec![
                        Operand{
                            r#type: "M".to_string(),
                            key: "IssuedGrid".bytes().collect(),
                            args: vec![]
                        }
                    ]
                },
                

                Instruction{
                    operation: "RETURN".to_string(),
                    write: None,
                    operands: vec![]
                }
            ]
        };
        let execution = Execution{
            cost: 0,
            executable: "TestUser1[CODE]=1234".to_string(),
            entity: "TestUser1".to_string(),
            inputs: Vec::new(),
            stack: vec![main_frame.clone()],
            generated: HashMap::new(),
            resources: HashMap::new(),
            caller: "TestUser2".to_string(),
            loaded: HashMap::new(),
            memory: MemorySpace{
                vars: HashMap::new()
            },
            balances: Vec::new()
        };
        
        let mut byte_code = Vec::<u8>::new();
        let new_frame_encoded = proto_bufs::ExecutableFrame{
            instructions: main_frame.instructions.clone()
        };
        new_frame_encoded.encode(&mut byte_code).unwrap();
        return execution
    }
    pub async fn execute_instruction(&mut  self, instruction: &Instruction, state: &mut state::StateSession, current_stack: &mut Frame) -> Option<Vec::<u8>>{
        let mut resolved: Vec<Vec<u8>> = Vec::new();
        for operand in instruction.operands.iter() {
            resolved.push(
                self.resolve(operand, current_stack, state).await.unwrap_or(vec![])
            );
        }
        let mut result: Option<Vec::<u8>> = None;
        match instruction.operation.as_str(){
            "ISSUE" => {
                result = self.issue(&resolved, state).await;
            },
            "LOAD" => {
                println!("Loading resource!");
                result = self.load_resource(&resolved).await;
            },
            "CASH" => {
                result = self.transfer(&resolved, state).await;
            }
            "GSET" => {
                result = self.set_global(&resolved).await;
            }
            "LSET" => {
                result = self.set_local(&resolved, current_stack).await;
            }
            "PRINT" => {
                println!("{:?}", &resolved);
                result = Some(vec![true as u8]);
            }
            "EQ" => {
                result = Some(
                    vec![(resolved[1] == resolved[0]) as u8]
                );
            }
            "LT" => {
                result = Some(
                    vec![(resolved[1] < resolved[0]) as u8]
                );
            }
            "GT" => {
                result =  Some(
                    vec![(resolved[1] > resolved[0]) as u8]
                );
            }
            "+"|"-"|"*" => {
                let result =  self.arithmethic(instruction.operation.as_str(), &resolved);
            }
            "SET" => {
                result = Some(resolved[0].clone());
            }
            "SIGN" => {
                //for convenienece
                result = self.sign(&resolved, current_stack, state).await;
            }
            "CSIGN" => {
                //check signature
                result = self.check_signature(&resolved, current_stack, state).await;
            }
            "RETURN" => {
                return Some(vec![]);
            }
            
            "JUMP" => {
                if resolved[0][0] == 1{
                    println!("Jumping...");
                    return Some(resolved[1].clone())
                }
                else{
                    return None
                }
            }
            
            _ => {
            }
        }
        if result.is_none(){
            return None;
        }

        if let Some(write_operand) = &instruction.write {
            let return_var = MemoryVar{
                key: write_operand.key.clone(),
                r#type: "STR".to_string(),
                value: result.as_ref().unwrap().to_vec()
            };
            let write = self.write( &instruction.write.as_ref().unwrap(), current_stack, state, return_var).await;
        }

        return result;
    }

    pub async fn run(&mut  self, state: &mut state::StateSession) -> bool{
        while let Some(mut current_stack) = self.stack.pop() {
            let instructions = std::mem::take(&mut current_stack.instructions);
            println!("Instructions: {}", instructions.len());
            let mut pos = 0 as usize;
            while pos < instructions.len(){
                let mut instruction = instructions.get(pos).unwrap();
                let success = self.execute_instruction(&instruction, state, &mut current_stack).await;
                if success.is_none() && instruction.operation.as_str() != "JUMP"{
                    println!("Failed at: {}", instruction.operation.as_str());
                    return false
                }
                else if !success.is_none() && instruction.operation.as_str() == "JUMP"{
                    pos = u64::from_le_bytes(success.unwrap()[..8].try_into().unwrap()) as usize;
                }
                else{
                    pos = pos+1;
                }
            }
        }
        return true
    }


    pub fn load_executable_resource(resource: Resource) -> Execution{
        let byte_code = resource.get_byte_field("bytecode");
        let new_executable = Execution::load_executable(byte_code.unwrap().clone());
        return new_executable;
    }

    pub fn load_executable(executable_encoded: Vec::<u8>) -> Execution{
        ////println!("Loading executable bytecode: {}", hex::encode(&executable_encoded));
        let executable = proto_bufs::ExecutableFrame::decode(&(executable_encoded)[..]).unwrap();
        let mut main_frame = Frame{
            memory: MemorySpace{
                vars: HashMap::new()
            },
            inputs: Vec::new(),
            returns: Vec::new(),
            instructions: executable.instructions
        };
        let execution = Execution{
            cost: 0,
            executable: "TestUser1[CODE]=1234".to_string(),
            entity: "TestUser1".to_string(),
            inputs: Vec::new(),
            stack: vec![main_frame.clone()],
            generated: HashMap::new(),
            resources: HashMap::new(),
            loaded: HashMap::new(),
            caller: "TestUser2".to_string(),
            memory: MemorySpace{
                vars: HashMap::new()
            },
            balances: Vec::new()
        };
        return execution
    }
    pub async fn load_call(call: &Resource) -> Option<Execution>{
        let executable_grid = call.get_text_field("executable");
        let caller = call.get_text_field("caller");
        let executable = Resource::find_resource(executable_grid.clone()).await;
        if executable.is_none(){
            println!("Executable grid not valid!");
            return None;
        }
        println!(
            "Executable call\n{}\nby {}",
            executable_grid.clone(),
            caller
        );
        let mut execution = Execution::load_executable_resource(executable.unwrap());
        execution.caller = caller;
        return Some(execution);
    }


   

    pub async fn execute(&mut  self, state: &mut state::StateSession){
        let executed = self.run(state).await;
        if !executed{
            return
        }
        for balance in self.balances.iter(){
            state.get_geid(&balance.geid, Some(balance.balance)).await;
        }
        for (pos, generated) in self.generated.clone(){
            println!("Generated: {}", generated.payload.as_ref().unwrap().grid);
        }
        self.text();
        return;
    }

    pub fn arithmethic(&mut self, operation: &str, operands: &Vec::<Vec::<u8>>) -> Option<Vec::<u8>>{
        let mut int_operands: Vec::<u64> = Vec::new();
        for operand in operands.iter(){
            if operand.len() < 8 {
                return None; // or handle error state
            }
            int_operands.push(u64::from_le_bytes(operand.clone()[..8].try_into().unwrap()));
        }
        let mut result = 0;
        match operation{
            "+" => {
                result = int_operands[0]+int_operands[1]
            }
            "-" => {
                result = int_operands[0]-int_operands[1]
            }
            "*" => {
                result = int_operands[0]*int_operands[1]
            }
            "/" => {
                result = int_operands[0]/int_operands[1]
            }

            _ => {}
        }
        return Some(result.to_le_bytes().to_vec());
    }
}

