use super::Execution as Execution;
use super::Frame as Frame;
use super::MemoryVar as MemoryVar;
use super::ResourceVar as ResourceVar;

use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;
use crate::signatures;

use crate::state;
use async_recursion::async_recursion;
use crate::Resource;
use crate::resources::ChainResource;
use crate::self_node;


impl Execution{

    pub async fn check_signature(&mut self, operands: &Vec::<Vec::<u8>>, frame: &mut Frame, state: &mut state::StateSession) -> Option<Vec::<u8>>{

        let resource_grid = String::from_utf8_lossy(&operands[0]).into_owned();
        let resource_var = self.resources.get(&resource_grid).cloned();
        let mut resource = resource_var.unwrap().resource;
        let loaded_key = signatures::load_key(resource.clone());

        let result = loaded_key.unwrap().check_signature_bytes(operands[2].clone(), operands[1].clone());
        println!("SIGN_RESSSS: {:?}", result);
        return Some(vec![true as u8])
    }

    pub async fn sign(&mut self, operands: &Vec::<Vec::<u8>>, frame: &mut Frame, state: &mut state::StateSession) -> Option<Vec::<u8>>{
        //for testing
        let resource_grid = String::from_utf8_lossy(&operands[0]).into_owned();
        let resource_var = self.resources.get(&resource_grid).cloned();
        let mut resource = resource_var.unwrap().resource;
        let loaded_key = signatures::load_key(resource.clone());


        let result = self_node::sign_resource(operands[1].clone()).await;
 
        return Some(result)
    }
}