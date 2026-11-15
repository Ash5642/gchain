use super::Execution as Execution;
use super::Frame as Frame;

use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

use crate::state;
use async_recursion::async_recursion;




impl Execution{
    pub fn text(&self){
        println!("Test passed!");
    }

    #[async_recursion]
    pub async fn resolve(&mut self, operand: &Operand, frame: &mut Frame, state: &mut state::StateSession) -> Option<Vec::<u8>>{
        match operand.r#type.as_str(){
            "D" => {
                return Some(operand.key.clone())
            }

            "M" => {
                let stack_var = frame.memory.vars.get(&operand.key);
                
                if !stack_var.is_none(){
                    println!("STACKVARR");
                    return Some(stack_var.unwrap().value.clone());
                }
                let global_var = self.memory.vars.get(&operand.key);
                if global_var.is_none(){
                    return None;
                }
                println!("STACKVARR");
                return Some(global_var.unwrap().value.clone())
            }

            "R" => {
                return Some(state.rand())
            }
        
            "CALL" => {
                let new_instruction = Instruction{
                    operation: String::from_utf8_lossy(&operand.key).into_owned(),
                    write: None,
                    operands: operand.args.clone()
                };
                let call_result = self.execute_instruction(
                    &new_instruction, state, frame
                ).await;

                return Some(call_result.unwrap_or(vec![]));
                
            }

            "E" => {
                let attribite = String::from_utf8_lossy(&operand.key).into_owned();
                match attribite.as_str(){
                    "geid" => {
                        return Some(self.caller.clone().into_bytes());
                    }
                    "call" => {
                        return Some(self.caller.clone().into_bytes());
                    }
                    _=>{}
                }
                
            }

            "G" => {
                return self.resolve_resource(operand, frame, state).await;
            }

            "S" => {
                //Give state information
                match String::from_utf8_lossy(&operand.key).into_owned().as_str(){
                    "height" => {
                        return Some(state.height.to_le_bytes().to_vec())
                    }
                    "sroot" => {
                        return Some(state.get_root().await);
                    }

                    _ =>{
                        
                    }
                }
            }

            _ => {
                return None;
            }
        }
        return None; 
    }
}