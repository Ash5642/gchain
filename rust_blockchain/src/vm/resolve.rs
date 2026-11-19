use super::Execution as Execution;
use super::Frame as Frame;
use super::MemoryVar as MemoryVar;
use super::DataType as DataType;

use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

use crate::state;
use async_recursion::async_recursion;




impl Execution{
    pub fn text(&self){
        //println!("Test passed!");
    }

    #[async_recursion]
    pub async fn resolve(&mut self, operand: &Operand, frame: &mut Frame, state: &mut state::StateSession) -> Option<MemoryVar>{
        match operand.r#type.as_str(){
            "D" => {
                return Some(
                    MemoryVar{
                        value:operand.key.clone(),
                        r#type: DataType::grid,
                        key: vec![]
                    }
                );
            }

            "M" => {
                
                if let Some(stack_var) = frame.memory.vars.get(&operand.key){
                    return Some(stack_var.clone());
                }
                if let Some(global_var) = self.memory.vars.get(&operand.key){
                    return Some(global_var.clone());
                }
                return None;
            }

            "R" => {
                return Some(
                    MemoryVar{
                        value:state.rand(),
                        r#type: DataType::bytes,
                        key: vec![]
                    }
                );
            }

            "A" => {
                let arg_var = self.args.get(&operand.key);
                if arg_var.is_none(){
                    return None;
                }
                return Some(
                    MemoryVar{
                        value:arg_var.unwrap().value.clone(),
                        r#type: DataType::bytes,
                        key: vec![]
                    }
                );
            }
        
            "CALL" => {
                let new_instruction = Instruction{
                    operation: 0,
                    write: None,
                    operands: operand.args.clone()
                };
                let call_result = self.execute_instruction(
                    &new_instruction, state, frame
                ).await;
                return Some(
                    MemoryVar{
                        value:call_result.unwrap_or(vec![]),
                        r#type: DataType::grid,
                        key: vec![]
                    }
                );
                
            }

            "E" => {
                let attribite = String::from_utf8_lossy(&operand.key).into_owned();
                match attribite.as_str(){
                    "geid" => {
                        return Some(
                            MemoryVar{
                                value:self.caller.clone().into_bytes(),
                                r#type: DataType::grid,
                                key: vec![]
                            }
                        );
                    }
                    "call" => {
                        return Some(
                            MemoryVar{
                                value:self.caller.clone().into_bytes(),
                                r#type: DataType::grid,
                                key: vec![]
                            }
                        );
                    }
                    "self" => {
                        return Some(
                            MemoryVar{
                                value:self.entity.clone().into_bytes(),
                                r#type: DataType::grid,
                                key: vec![]
                            }
                        );
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
                        return Some(
                            MemoryVar{
                                value:state.height.to_le_bytes().to_vec(),
                                r#type: DataType::grid,
                                key: vec![]
                            }
                        );
                    }
                    "sroot" => {
                        return Some(
                            MemoryVar{
                                value:state.get_root().await,
                                r#type: DataType::grid,
                                key: vec![]
                            }
                        );
                    }

                    _ =>{
                        
                    }
                }
            }
            "CS" => {
                return Some(
                    MemoryVar{
                        value:vec![],
                        r#type: DataType::grid,
                        key: vec![]
                    }
                );
            }
            "ES" => {
                return Some(
                    MemoryVar{
                        value:vec![],
                        r#type: DataType::grid,
                        key: vec![]
                    }
                );
            }



            _ => {
                return None;
            }
        }
        return None; 
    }
}