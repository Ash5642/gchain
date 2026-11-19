use super::Execution as Execution;
use super::Frame as Frame;
use super::Balance as Balance;
use super::ResourceVar as ResourceVar;
use crate::resources;
use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

use crate::state;

use async_recursion::async_recursion;




impl Execution{
    pub async fn transfer(&mut self, operands: &Vec::<Vec::<u8>>, state: &mut state::StateSession) -> Option<Vec::<u8>>{

        let reciever =  String::from_utf8_lossy(&operands[0]).into_owned();
        let sender = self.entity.clone();
        let amt: u64;
        if operands[1].len() == 8{
            amt = u64::from_le_bytes(operands[1].clone()[..8].try_into().unwrap());
        }
        else{
            amt = 0 as u64;
        }
        let balance = self.get_balance(&sender, state).await;
        if balance < amt{
            return None;
        }

        let sender_balance = self.update_balance(&sender, false, amt, state).await;
        let reciever_balance = self.update_balance(&reciever, true, amt, state).await;
        let identifier_raw = state.rand();
        let identifier = hex::encode(identifier_raw);
        let generated_transaction = crate::Resource::create_transaction(&self.entity, &reciever, amt, &identifier).await;

        self.resources.insert(
            generated_transaction.payload.as_ref().unwrap().grid.clone(), 
            ResourceVar{
                mutable: true,
                resource: generated_transaction.clone()
            }
            
        );
        Some(generated_transaction.payload.as_ref().unwrap().grid.clone().bytes().collect())
    }

    pub async fn recieve(&mut self, operands: &Vec::<Vec::<u8>>, state: &mut state::StateSession) -> Option<Vec::<u8>>{

        let sender =  String::from_utf8_lossy(&operands[0]).into_owned();
        let reciever = self.entity.clone();

        let amt: u64;
        if operands[1].len() == 8{
            amt = u64::from_le_bytes(operands[1].clone()[..8].try_into().unwrap());
        }
        else{
            amt = 0 as u64;
        }
        let balance = self.get_balance(&sender, state).await;
        if balance < amt{
            return None;
        }

        let sender_balance = self.update_balance(&sender, false, amt, state).await;
        let reciever_balance = self.update_balance(&reciever, true, amt, state).await;
        let identifier_raw = state.rand();
        let identifier = hex::encode(identifier_raw);
        let generated_transaction = crate::Resource::create_transaction(&self.entity, &reciever, amt, &identifier).await;

        self.resources.insert(
            generated_transaction.payload.as_ref().unwrap().grid.clone(), 
            ResourceVar{
                mutable: true,
                resource: generated_transaction.clone()
            }
            
        );
        Some(generated_transaction.payload.as_ref().unwrap().grid.clone().bytes().collect())
    }


    pub async fn get_balance(&mut self, entity: &str, state: &mut state::StateSession) -> u64{
        for balance in self.balances.iter(){
            if balance.geid == entity{
                return balance.balance
            }
        }
        let record = state.read_record(entity).await;
        if record.is_none(){
            return 0
        }
        let balance = record.as_ref().unwrap().balance;
        //println!("READ BAA: {}", balance);
        self.balances.push(
            Balance{
                geid: entity.to_string(),
                balance: balance
            }
        );
        return record.as_ref().unwrap().balance;
    }

    pub async fn update_balance(&mut self, entity: &str, incoming: bool, amt: u64, state: &mut state::StateSession) -> u64{
        self.get_balance(entity, state).await;
        for balance in self.balances.iter_mut(){
            if balance.geid == entity{
                if incoming{
                    balance.balance = balance.balance + amt;
                }
                else{
                    balance.balance = balance.balance - amt;
                }
                return balance.balance;
            }
        }
        return 0;
    }
}