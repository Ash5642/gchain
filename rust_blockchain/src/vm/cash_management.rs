use super::Execution as Execution;
use super::Frame as Frame;
use super::Balance as Balance;
use super::ResourceVar as ResourceVar;
use crate::resources;
use crate::proto_bufs::Resource as Resource;

use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

use crate::state;

use async_recursion::async_recursion;




impl Execution{

    pub async fn transfer(&mut self, operands: &Vec::<Vec::<u8>>, state: &mut state::StateSession) -> Option<Vec::<u8>>{
        
        let sender = String::from_utf8_lossy(&operands[1]).into_owned();
        let reciever = String::from_utf8_lossy(&operands[2]).into_owned();
        let amt = u64::from_le_bytes(operands[0].clone()[..8].try_into().unwrap());

        let mut sender_balance = self.get_balance(&sender, state).await;
        let mut reciever_balance = self.get_balance(&reciever, state).await;

        if sender_balance<amt{
            return None;
        }

        let identifier = hex::encode(state.rand());
        let field_values = vec![
            sender.clone().into_bytes(),
            reciever.clone().into_bytes(),
            amt.to_le_bytes().to_vec(),
        ];
        let new_transaction = Resource::create_resource(&sender, resources::structures::ResourceCodes::Transaction, &identifier, &field_values, state);

        let sender_balance = sender_balance - amt;
        let reciever_balance = reciever_balance + amt;

        if let Some(transaction) = new_transaction{
            self.update_balance(&sender, sender_balance, state);
            self.update_balance(&reciever, reciever_balance, state);
            return Some(transaction.payload.as_ref().unwrap().grid.clone().into_bytes());
        }
        return None;
    }

    pub async fn get_balance(&mut self, entity: &str, state: &mut state::StateSession) -> u64{

        if let Some(balance) = self.balances.get(entity){
            return balance.balance;
        }
        if let Some(balance) = state.read_record(entity).await{
            self.balances.insert(
                entity.to_string(),
                Balance{
                    geid: entity.to_string(),
                    balance: balance.balance
                }
            );
            return balance.balance;
        }
        return 0

    }

    pub async fn update_balance(&mut self, geid: &str, balance: u64, state: &mut state::StateSession){
        self.balances.insert(
            geid.to_string(),
            Balance{
                geid: geid.to_string(),
                balance: balance
            }
        );
    }


}
