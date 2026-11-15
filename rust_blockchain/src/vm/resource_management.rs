use super::Execution as Execution;
use super::Frame as Frame;
use super::MemoryVar as MemoryVar;
use super::ResourceVar as ResourceVar;

use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

use crate::state;
use async_recursion::async_recursion;
use crate::Resource;
use crate::resources::ChainResource;



impl Execution{
    pub async fn resolve_resource(&mut self, operand: &Operand, frame: &mut Frame, state: &mut state::StateSession) -> Option<Vec::<u8>>{
        let grid_encoded = self.resolve(&operand.args[0], frame, state).await;
        if grid_encoded.is_none(){
            return None
        }
        let resource_grid = String::from_utf8_lossy(&grid_encoded.as_ref().unwrap()).into_owned();
        println!("Loading {}", resource_grid);
        let resource_wrapped = self.resources.get(&resource_grid).cloned();

        let mut resource = resource_wrapped.unwrap().resource;
        if operand.args.len() < 2{
            println!("Can't address resource!");
        }
        let attribute_encoded = self.resolve(&operand.args[1], frame, state).await;
        if attribute_encoded.is_none(){
            return None
        }
        match String::from_utf8_lossy(&attribute_encoded.as_ref().unwrap()).into_owned().as_str(){
            "grid" => {
                return Some(resource_grid.into_bytes());
            }
            "status" => {
                return Some(resource.status.to_le_bytes().to_vec());
            }
            "block" => {
                for proof in resource.proof.iter(){
                    if proof.r#type == "+"{
                        return Some(proof.block.to_le_bytes().to_vec())
                    }
                }
                return None
            }
            "field" => {
                let key_encoded = self.resolve(&operand.args[2], frame, state).await;
                if key_encoded.is_none(){
                    return None;
                }
                
                let key = String::from_utf8_lossy(&key_encoded.as_ref().unwrap());
                let field_value =  resource.get_field_bytes(&key);
                return Some(field_value);
            }
            _ => {
                return None
            }
        }
    }


    pub async fn load_resource(&mut self, operands: &Vec::<Vec::<u8>>) -> Option<Vec::<u8>>{
        let resource_grid = String::from_utf8_lossy(&operands[0]).into_owned();
        let resource_wrapped = Resource::find_resource(resource_grid.clone()).await;
        if resource_wrapped.is_none(){
            eprintln!("Resource GRID invalid!");
            return None;
        }
        println!("Loaded succssfully!");
        let new_resource_var = ResourceVar{
            mutable: false,
            resource: resource_wrapped.unwrap()
        };
        self.resources.insert(resource_grid, new_resource_var);
        return Some(operands[0].clone())
    }

    pub async fn get_resource(&mut self, operands: &Vec::<Vec::<u8>>) -> Option<Resource>{
        let resource_grid = String::from_utf8_lossy(&operands[0]).into_owned();

        let mut resource_var = self.resources.get(&resource_grid).cloned();
        let mut resource;
        if !resource_var.is_none(){
            self.load_resource(operands).await;
            resource_var = self.resources.get(&resource_grid).cloned();
        }
        resource = resource_var.unwrap().resource;
        
         
        return Some(resource)
    }


    pub async fn issue(&mut self, operands: &Vec::<Vec::<u8>>, state: &mut state::StateSession) -> Option<Vec::<u8>>{
        println!(
            "issuing new resource\n{}",
            String::from_utf8_lossy(&operands[0]).into_owned()
        );
        let resource_type = String::from_utf8_lossy(&operands[0]).into_owned();
    
        let identifier_key = hex::encode(state.rand());
        let new_resource = Resource::create_resource(&self.entity, &resource_type, &identifier_key);

        self.resources.insert(
            new_resource.payload.as_ref().unwrap().grid.clone(), 
            ResourceVar{
                mutable: true,
                resource: new_resource.clone()
            }
        );
        return Some(new_resource.payload.as_ref().unwrap().grid.bytes().collect())
    }


    pub async fn set_field(&mut self, operand: &Operand, frame: &mut Frame, state: &mut state::StateSession, value: MemoryVar) -> Option<Vec::<u8>>{
        let grid_encoded = self.resolve(&operand.args[0], frame, state).await;
        if grid_encoded.is_none(){
            return None
        }
        let resource_grid = String::from_utf8_lossy(&grid_encoded.as_ref().unwrap()).into_owned();
        let resource_var = self.resources.get(&resource_grid).cloned();
        if !resource_var.as_ref().unwrap().mutable{
            println!("Can't modify immutable resource!");
            return None
        }

        let mut resource = resource_var.unwrap().resource;

        let attribute_encoded = self.resolve(&operand.args[1], frame, state).await;
        if attribute_encoded.is_none(){
            return None
        }
        match String::from_utf8_lossy(&attribute_encoded.as_ref().unwrap()).into_owned().as_str(){
            "field" => {

                let key_encoded = self.resolve(&operand.args[2], frame, state).await;
                if key_encoded.is_none(){
                    return None;
                }
                if let Some(payload) = resource.payload.as_mut() {
                    for field in payload.fields.iter_mut() {
                        if String::from_utf8_lossy(&key_encoded.clone().unwrap()) == field.key{
                            field.value = value.value.clone();
                            return None
                        }
                    }
                }
            }

            _ =>{ 
                return None
            }
        }

        let new_resource_var = ResourceVar{
            mutable: true,
            resource: resource.clone()
        };
        self.resources.insert(String::from_utf8_lossy(&grid_encoded.as_ref().unwrap()).into_owned(), new_resource_var);
        return None
    }
}