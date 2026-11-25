use tokio_rusqlite::{params, Connection, Result};
use std::path::Path;
use crate::proto_bufs::Resource as Resource;
use crate::proto_bufs::Payload as Payload;
use crate::proto_bufs::MerkleProof as MerkleProof;
use crate::proto_bufs;
use crate::proto_bufs::Field as Field;
use crate::proto_bufs::Header as HeaderProtobuf;
use prost::Message;
use crate::blocks;
use std::fs;
use crate::blocks::merkle::MerkleNode;



use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use crate::resources;
use rand::RngExt;
use rand::Rng;
use sha2::{Sha256, Digest};
use crate::self_node;
use std::env;
use crate::utils;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task;
use fastbloom::BloomFilter;
use std::sync::{Mutex, LazyLock, Arc};
use tokio::sync::RwLock;
use crate::resources::signatures;

use rocksdb::{DB, Options, FlushOptions, WriteOptions};
use std::sync::OnceLock;
use tokio::sync::OnceCell;
use async_recursion::async_recursion;
use crate::state;
use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;
mod primitive;
mod resolve; 
mod resource_management; 
mod memory;
mod cash_management;
mod vm_cryptography;



mod DataType {
    pub const null:u8 = 0;
    pub const grid:u8 = 1;
    pub const text:u8 = 2;
    pub const uint:u8 = 3;
    pub const bytes:u8 = 4;
    pub const boolean:u8 = 5;
}


#[derive(Clone)]

pub struct ExecutionIO{
    key: String,
    r#type: u8,
    resource: Option<Resource>
}

#[derive(Clone)]
pub struct ResourceVar{
    pub resource: Resource,
    pub mutable: bool
}


#[derive(Clone)]
#[derive(Debug)]

pub struct MemoryVar{
    key: Vec::<u8>,
    r#type: u8,
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

mod OpCode {
    pub const Set:u32 = 0;
    pub const Add:u32 = 1;
    pub const Sub:u32 = 2;
    pub const Mul:u32 = 3;
    pub const Div:u32 = 4;

    pub const Load:u32 = 32;
    pub const Cash:u32 = 33;
    pub const Issue:u32 = 34;
    pub const Sign:u32 = 64;
    pub const CheckSign: u32 = 65;
    pub const TakeCash:u32 = 35;

    pub const Print:u32 = 66;
    pub const PrintStr:u32 = 67;

    pub const Eq:u32 = 128;
    pub const LT:u32 = 129;
    pub const GT:u32 = 130;

    pub const Exit:u32 = 256;
    pub const Jump:u32 = 257;

    pub const Concat:u32 = 512;
}



#[derive(Clone)]
pub struct Execution{
    executable: String,
    entity: String,
    inputs: Vec::<ExecutionIO>,
    memory: MemorySpace,
    pub generated: HashMap<String, Resource>,
    pub revoked: Vec::<String>,
    pub loaded: HashMap<String, Resource>,
    pub resources: HashMap<String, ResourceVar>,
    pub args: HashMap<Vec<u8>, MemoryVar>,
    stack: Vec::<Frame>,
    balances: HashMap::<String, Balance>,
    caller: String,
    call_grid: String,
    cost: u64,
    pub success: bool
}


impl Execution{

    pub async fn execute_instruction(&mut  self, instruction: &Instruction, state: &mut state::StateSession, current_stack: &mut Frame) -> Option<Vec::<u8>>{
        let mut resolved: Vec<Vec<u8>> = Vec::new();
        for operand in instruction.operands.iter() {
            if let Some(resolve_result) = self.resolve(operand, current_stack, state).await{
                resolved.push(resolve_result.value);
            }
            else{
                return None
            }
        }
        let mut result: Option<Vec::<u8>> = None;
        let mut result_type: u8;
        match instruction.operation{
            OpCode::Issue => {
                result = self.issue(&resolved, state).await;
                result_type =  DataType::grid;
            },
            OpCode::Load => {
                result = self.load_resource(&resolved).await;
                result_type =  DataType::grid;
            },
            OpCode::Cash => {
                result = self.transfer(&resolved, state).await;
                result_type =  DataType::grid;
            }
            OpCode::TakeCash => {
                result = None;
                result_type =  DataType::grid;
            }

            OpCode::Print => {
                println!("{:?}", &resolved);
                result = Some(vec![true as u8]);
                result_type =  DataType::boolean;
            }
            OpCode::PrintStr => {
                println!("{:?}", String::from_utf8_lossy(&resolved[0]));
                result = Some(vec![true as u8]);
                result_type =  DataType::boolean;
            }
            OpCode::Eq => {
                result = Some(
                    vec![(resolved[1] == resolved[0]) as u8]
                );
                result_type =  DataType::boolean;
            }
            OpCode::LT => {
                result = Some(
                    vec![(resolved[1] < resolved[0]) as u8]
                );
                result_type =  DataType::boolean;
            }
            OpCode::GT => {
                result =  Some(
                    vec![(resolved[1] > resolved[0]) as u8]
                );
                result_type =  DataType::boolean;
            }
            OpCode::Add|OpCode::Sub|OpCode::Mul|OpCode::Div =>{
                result =  self.arithmethic(instruction.operation, &resolved);
                result_type = DataType::uint;
            }

            OpCode::Set => {
                result = Some(resolved[0].clone());
                result_type =  DataType::grid;
            }
            OpCode::Sign => {
                //for convenienece
                result = self.sign(&resolved, current_stack, state).await;
                result_type =  DataType::grid;
            }
            OpCode::CheckSign => {
                //check signature
                result = self.check_signature(&resolved, current_stack, state).await;
                result_type =  DataType::boolean
            }
            OpCode::Exit => {
                result_type =  DataType::grid;
                return Some(vec![]);
            }
            OpCode::Concat => {
                result = Some(resolved.concat());
                result_type =  DataType::grid;
            }
            
            OpCode::Jump => {
                if resolved[0][0] == 1{
                    return Some(resolved[1].clone())
                }
                else{
                    return None
                }
            }
            
            _ => {
                result_type = 100;
            }
        }
        if result.is_none(){
            return None;
        }

        if let Some(write_operand) = &instruction.write {
            let return_var = MemoryVar{
                key: write_operand.key.clone(),
                r#type: result_type,
                value: result.as_ref().unwrap().to_vec()
            };
            let write = self.write( &instruction.write.as_ref().unwrap(), current_stack, state, return_var).await;
        }

        return result;
    }

    pub async fn run(&mut  self, state: &mut state::StateSession) -> bool{
        while let Some(mut current_stack) = self.stack.pop() {
            let instructions = std::mem::take(&mut current_stack.instructions);
            //println!("Instructions: {}", instructions.len());
            let mut byte_code = Vec::<u8>::new();
            let mut new_frame_encoded = proto_bufs::ExecutableFrame{
                instructions: instructions.clone(),
                arguments: vec![]
            };
            for (key, arg) in self.args.clone(){
                new_frame_encoded.arguments.push(
                    proto_bufs::Argument{
                        r#type: 0,
                        key: arg.key.clone()
                    }
                )
            };
            new_frame_encoded.encode(&mut byte_code).unwrap();
            println!("Bytecode: {}", hex::encode(&byte_code));
            let mut pos = 0 as usize;
            while pos < instructions.len(){
                let mut instruction = instructions.get(pos).unwrap();
                let success = self.execute_instruction(&instruction, state, &mut current_stack).await;
                if success.is_none() && instruction.operation != OpCode::Jump{
                    //println!("Failed at: {}", instruction.operation);
                    return false
                }
                else if !success.is_none() && instruction.operation == OpCode::Jump{
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
        //////println!("Loading executable bytecode: {}", hex::encode(&executable_encoded));
        let executable = proto_bufs::ExecutableFrame::decode(&(executable_encoded)[..]).unwrap();
        let mut main_frame = Frame{
            memory: MemorySpace{
                vars: HashMap::new()
            },
            inputs: Vec::new(),
            returns: Vec::new(),
            instructions: executable.instructions
        };
        let mut execution = Execution{
            cost: 0,
            call_grid: String::from("TestUser2[CALL]=1234"),
            executable: "TestUser1[CODE]=1234".to_string(),
            entity: "TestUser1".to_string(),
            inputs: Vec::new(),
            stack: vec![main_frame.clone()],
            generated: HashMap::new(),
            resources: HashMap::new(),
            loaded: HashMap::new(),
            caller: "TestUser2".to_string(),
            args: HashMap::new(),
            revoked: Vec::new(),
            memory: MemorySpace{
                vars: HashMap::new()
            },
            balances: HashMap::new(),
            success: false,
        };
        for argument in executable.arguments.iter(){
            execution.args.insert(
                argument.key.clone(), 
                MemoryVar{
                    r#type: 0,
                    key: argument.key.clone(),
                    value: vec![]

                }
            );
        }
        return execution
    }
    pub async fn load_call(call: &Resource) -> Option<Execution>{
        let executable_grid = call.get_text_field("executable");
        let caller = call.get_text_field("caller");
        //println!("Finding...");
        let executable = Resource::find_resource(executable_grid.clone()).await;
        //println!("FOund!");
        if executable.is_none(){
            println!("Executable grid not valid!");
            return None;
        }
        let mut execution = Execution::load_executable_resource(executable.unwrap());
        execution.call_grid = call.payload.as_ref().unwrap().grid.clone();
        let arguments_encoded = call.get_byte_field("arguments").unwrap();
        let arguments = proto_bufs::CallArguments::decode(&arguments_encoded[..]).unwrap();
        for argument in arguments.arguments.iter(){
            execution.args.insert(
                argument.key.clone(),
                MemoryVar{
                    key: argument.key.clone(),
                    r#type: 0,
                    value: argument.value.clone()
                }
            );
        };
        execution.caller = caller;
        return Some(execution);
    }


   

    pub async fn execute(&mut  self, state: &mut state::StateSession){
        let executed = self.run(state).await;
        if !executed{
            self.success = false;
            return
        }
        for (geid, balance) in self.balances.iter(){
            state.get_geid(&geid, Some(balance.balance)).await;
        }
        for (pos, generated) in self.resources.clone(){
            //println!("Generated: {} {}", generated.resource.payload.as_ref().unwrap().grid, self.call_grid);
        }
        self.success = true;
        return;
    }

    pub fn arithmethic(&mut self, operation: u32, operands: &Vec::<Vec::<u8>>) -> Option<Vec::<u8>>{
        let mut int_operands: Vec::<u64> = Vec::new();
        for operand in operands.iter(){
            if operand.len() < 8 {
                return None;
            }
            int_operands.push(u64::from_le_bytes(operand.clone()[..8].try_into().unwrap()));
        }
        let mut result = 0;
        match operation{
            OpCode::Add => {
                result = int_operands[0]+int_operands[1]
            }
            OpCode::Sub => {
                result = int_operands[0]-int_operands[1]
            }
            OpCode::Mul => {
                result = int_operands[0]*int_operands[1]
            }
            OpCode::Div => {
                result = int_operands[0]/int_operands[1]
            }

            _ => {}
        }
        return Some(result.to_le_bytes().to_vec());
    }
}

