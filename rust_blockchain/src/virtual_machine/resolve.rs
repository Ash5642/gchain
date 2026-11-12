


pub fn resolve(&mut self, operand: &Operand) -> Vec::<u8>{
    match operand.r#type.as_str(){
        "D" => {
            return operand.key.clone()
        }

        _ => {
            return Vec::new();
        }
    }
    return Vec::new(); 
}