use super::Execution as Execution;
use super::Frame as Frame;
use super::Balance as Balance;
use super::ResourceVar as ResourceVar;

use std::collections::HashMap;
use crate::proto_bufs::Operand as Operand;
use crate::proto_bufs::Instruction as Instruction;

use crate::state;

use async_recursion::async_recursion;




impl Execution{
    pub async fn strcat(&mut self, operands: &Vec::<Vec::<u8>>) -> Option<Vec::<u8>>{
        return Some(operands.concat());
    }

}