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


#[derive(Clone)]

pub struct ExecutionIO{
    key: String,
    r#type: u8,
    resource: Option<Resource>
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
    pub generated: Vec::<Resource>,
    stack: Vec::<Frame>,
    balances: Vec::<Balance>,
    caller: String
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
                    operation: "GSET".to_string(),
                    operands: vec![
                        Operand{
                            r#type: "D".to_string(),
                            key: "test_var".bytes().collect()
                        },
                        Operand{
                            r#type: "D".to_string(),
                            key: "str".bytes().collect()
                        },
                        Operand{
                            r#type: "D".to_string(),
                            key: "test_val".bytes().collect()
                        }
                    ]
                },
                Instruction{
                    operation: "ISSUE".to_string(),
                    operands: vec![
                        Operand{
                            r#type: "D".to_string(),
                            key: "GRANT".bytes().collect()
                        }
                    ]
                },
                Instruction{
                    operation: "CASH".to_string(),
                    operands: vec![
                        Operand{
                            r#type: "M".to_string(),
                            key: "test_var".bytes().collect()
                        },
                        Operand{
                            r#type: "R".to_string(),
                            key: u64::from(5 as u64).to_le_bytes().to_vec()
                        }
                    ]
                },
                Instruction{
                    operation: "RETURN".to_string(),
                    operands: vec![]
                }
            ]
        };
        let execution = Execution{
            executable: "GovtOfIndia[CODE]=1234".to_string(),
            entity: "GovtOfIndia".to_string(),
            inputs: Vec::new(),
            stack: vec![main_frame.clone()],
            generated: Vec::new(),
            caller: "test_caller".to_string(),
            memory: MemorySpace{
                vars: HashMap::new()
            },
            balances: Vec::new()
        };
        println!("Executable loaded!");
        
        let mut byte_code = Vec::<u8>::new();
        let new_frame_encoded = proto_bufs::ExecutableFrame{
            instructions: main_frame.instructions.clone()
        };
        new_frame_encoded.encode(&mut byte_code).unwrap();
        println!("Byte code: {}", hex::encode(byte_code));
        return execution
    }


    pub async fn run(&mut  self, state: &mut state::StateSession) -> bool{
        while let Some(mut current_stack) = self.stack.pop() {
            let instructions = std::mem::take(&mut current_stack.instructions);
            for instruction in instructions.iter() {
                let mut resolved: Vec<Vec<u8>> = Vec::new();
                for operand in instruction.operands.iter() {
                    resolved.push(
                        self.resolve(operand, &mut current_stack, state)
                    );
                }
                let mut success = false;
                match instruction.operation.as_str(){
                    "ISSUE" => {
                        success = self.issue(&resolved, state).await;
                    },
                    "CASH" => {
                        success = self.transfer(&resolved, state).await;
                    }
                    "GSET" => {
                        success = self.set_global(&resolved).await;
                    }
                    "RETURN" => {
                        return true
                    }
                    
                    _ => {
                        success = false
                    }
                }
                if !success{
                    println!("Failed at: {}", instruction.operation.as_str());
                }
            }
        }
        println!("Executed!");
        return false
    }


    pub fn load_executable_resource(resource: Resource) -> Execution{
        let byte_code = resource.get_byte_field("bytecode");
        let new_executable = Execution::load_executable(byte_code.unwrap().clone());
        return new_executable;
    }

    pub fn load_executable(executable_encoded: Vec::<u8>) -> Execution{
        println!("Loading executable bytecode: {}", hex::encode(&executable_encoded));
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
            executable: "GovtOfIndia[CODE]=1234".to_string(),
            entity: "GovtOfIndia".to_string(),
            inputs: Vec::new(),
            stack: vec![main_frame.clone()],
            generated: Vec::new(),
            caller: "test_caller".to_string(),
            memory: MemorySpace{
                vars: HashMap::new()
            },
            balances: Vec::new()
        };
        return execution
    }
    pub async fn load_call(call: Resource) -> Execution{
        let executable_grid = call.get_text_field("executable");
        let caller = call.get_text_field("caller");
        let executable = Resource::find_resource(executable_grid).await;
        if executable.is_none(){
            println!("Executable grid not valid!");
        }
        let mut execution = Execution::load_executable_resource(executable.unwrap());
        execution.caller = caller;
        return execution;
    }




    pub fn resolve(&mut self, operand: &Operand, frame: &mut Frame, state: &mut state::StateSession) -> Vec::<u8>{
        match operand.r#type.as_str(){
            "D" => {
                return operand.key.clone()
            }

            "M" => {
                let stack_var = frame.memory.vars.get(&operand.key);
                if !stack_var.is_none(){
                    return stack_var.unwrap().value.clone();
                }
                let global_var = self.memory.vars.get(&operand.key);
                if global_var.is_none(){
                    println!("Variable not initalized!");
                    return Vec::new();
                }
                return global_var.unwrap().value.clone()
            }

            "R" => {
                return state.rand()
            }

            "E" => {
                let attribite = String::from_utf8_lossy(&operand.key).into_owned();
                match attribite.as_str(){
                    "geid" => {
                        println!("Reading caller entity!");
                        return self.caller.clone().into_bytes();
                    }
                    _=>{}
                }
                
            }

            _ => {
                return Vec::new();
            }
        }
        return Vec::new(); 
    }


    pub async fn issue(&mut self, operands: &Vec::<Vec::<u8>>, state: &mut state::StateSession) -> bool{
        println!(
            "issuing new resource\n{}",
            String::from_utf8_lossy(&operands[0]).into_owned()
        );
        let resource_type = String::from_utf8_lossy(&operands[0]).into_owned();
        let new_resource = Resource::create_resource(&self.entity, &resource_type);
        let identifier_key = hex::encode(state.rand());
        println!("Created new resouece: {}", new_resource.payload.as_ref().unwrap().grid);
        true
    }
   

    pub async fn execute(&mut  self, state: &mut state::StateSession){
        let executed = self.run(state).await;
        if !executed{
            return
        }
        for balance in self.balances.iter(){
            println!("Balance\t{}\t{}", balance.geid, balance.balance);
            state.get_geid(&balance.geid, Some(balance.balance)).await;
        }
        println!("Contract executed successfully!");
        return;

    }

    pub async fn transfer(&mut self, operands: &Vec::<Vec::<u8>>, state: &mut state::StateSession) -> bool{

        let reciever =  String::from_utf8_lossy(&operands[0]).into_owned();
        let sender = self.entity.clone();
        let amt = u64::from_le_bytes(operands[1].clone()[..8].try_into().unwrap());
        println!("Transferring {}", amt);
        let balance = self.get_balance(&sender).await;
        if balance < amt{
            return false;
        }

        let sender_balance = self.update_balance(&sender, false, amt, state).await;
        let reciever_balance = self.update_balance(&reciever, true, amt, state).await;
        let identifier_raw = state.rand();
        let identifier = hex::encode(identifier_raw);
        let generated_transaction = cash::Cash::create_transaction(&self.entity, &reciever, amt, &identifier).await;
        println!("New transaction: {}", generated_transaction.payload.as_ref().unwrap().grid);
        self.generated.push(
            generated_transaction
        );
        true
    }

    pub async fn set_global(&mut self, operands: &Vec::<Vec::<u8>>) -> bool{
        let new_var = MemoryVar{
            key: operands[0].clone(),
            r#type: String::from_utf8_lossy(&operands[1]).into_owned(),
            value: operands[2].clone()
        };
        self.memory.vars.insert(operands[0].clone(), new_var);
        true
    }

    pub async fn set_local(&mut self, operands: &Vec::<Vec::<u8>>, frame: &mut Frame) -> bool{
        let new_var = MemoryVar{
            key: operands[0].clone(),
            r#type: String::from_utf8_lossy(&operands[1]).into_owned(),
            value: operands[2].clone()
        };
        frame.memory.vars.insert(operands[0].clone(), new_var);
        true
    }














    pub async fn get_balance(&mut self, entity: &str) -> u64{
        for balance in self.balances.iter(){
            if balance.geid == entity{
                return balance.balance
            }
        }
        let record = state::read_record(entity).await;
        if record.is_none(){
            return 0
        }
        let balance = record.as_ref().unwrap().balance;
        self.balances.push(
            Balance{
                geid: entity.to_string(),
                balance: balance
            }
        );
        return record.as_ref().unwrap().balance;
    }

    pub async fn update_balance(&mut self, entity: &str, incoming: bool, amt: u64, state: &mut state::StateSession) -> u64{
        println!("Sending to {}", entity);
        self.get_balance(entity).await;
        for balance in self.balances.iter_mut(){
            if balance.geid == entity{
                if incoming{
                    balance.balance = balance.balance + amt;
                }
                else{
                    balance.balance = balance.balance - amt;
                }
                return balance.balance
            }
        }
        return 0
    }
}

