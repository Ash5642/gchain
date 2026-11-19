use super::Execution as Execution;
use super::Frame as Frame;
use super::MemoryVar as MemoryVar;

use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

use crate::state;
use async_recursion::async_recursion;




impl Execution{
    pub async fn set_global(&mut self, operands: &Vec::<Vec::<u8>>) -> Option<Vec::<u8>>{
        let new_var = MemoryVar{
            key: operands[0].clone(),
            r#type: 0,
            value: operands[2].clone()
        };
        self.memory.vars.insert(operands[0].clone(), new_var);
        Some(vec![true as u8])
    }

    pub async fn set_local(&mut self, operands: &Vec::<Vec::<u8>>, frame: &mut Frame) -> Option<Vec::<u8>>{
        let new_var = MemoryVar{
            key: operands[0].clone(),
            r#type: 0,
            value: operands[2].clone()
        };
        frame.memory.vars.insert(operands[0].clone(), new_var);
        Some(vec![true as u8])
    }

    #[async_recursion]
    pub async fn write(&mut self, operand: &Operand, frame: &mut Frame, state: &mut state::StateSession, value: MemoryVar) -> bool{
        match operand.r#type.as_str(){
            "M" => {
                frame.memory.vars.insert(operand.key.clone(), value.clone());
                return true
            }
            "G" => {
                self.set_field(operand, frame, state, value).await;
                return true;
            }
            _ => {
                //println!("Writing to immutable space!");
            }
        }
        return false; 
    }
}