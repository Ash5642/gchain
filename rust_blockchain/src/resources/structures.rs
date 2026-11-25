mod ResourceFieldType {
    pub const bytes: u32 = 0;
    pub const text: u32 = 1;
    pub const gid: u32 = 2;
    pub const uint: u32 = 3;
}

pub struct ResourceField {
    pub key: &'static str,
    pub r#type: u32,
    pub descriptor: &'static str, 
    pub required: bool,
    pub id: u32,
}


pub struct ResourceType {
    pub id: u32,
    pub name: &'static str, 
    pub fields: &'static [ResourceField],
    pub flags: &'static [&'static str],
    pub permissions: Option<[[bool; 5]; 3]>
}


pub mod ResourceCodes {
    pub const Key:u32 = 1;
    pub const Record:u32 = 2;
    pub const Transaction:u32 = 3;
    pub const Call:u32 = 4;
    pub const Revoke:u32 = 5;
    pub const Signature:u32 = 6;
    pub const LocalRoot:u32 = 7;
}

pub mod PermisionTypeCode {
    pub const Issue:usize = 1;
    pub const Revoke:usize = 2;
}

pub mod PermissionCode {
    pub const Public:usize = 0;
    pub const Executable:usize = 1;
    pub const Direct:usize = 2;
    pub const Parent:usize = 3;
    pub const Custom:usize = 3;
}

pub mod ResourceTypes {
    use super::{ResourceField, ResourceFieldType, ResourceType};

    pub const Key: ResourceType = ResourceType {
        name:"key",
        id: 0,
        flags: &[],
        permissions: None,
        fields: &[
            ResourceField {
                key: "public_key",
                r#type: ResourceFieldType::bytes,
                descriptor: "",
                required: true,
                id: 1,
            },
            ResourceField {
                key: "private_key",
                r#type: ResourceFieldType::bytes,
                descriptor: "",
                required: true,
                id: 2,
            },
            ResourceField {
                key: "type",
                r#type: ResourceFieldType::text,
                descriptor: "",
                required: true,
                id: 3,
            }
        ],
    };

    pub const Record: ResourceType = ResourceType {
        id: 1,
        name:"record",
        flags: &[],
        permissions: Some(
            [
                [
                    true, true, false, true, false
                ],
                [
                    true, true, true, true, true
                ],
                [
                    true, true, true, true, true
                ]

            ]
        ),
        fields: &[
            ResourceField {
                key: "type",
                r#type: ResourceFieldType::text,
                descriptor: "",
                required: true,
                id: 1,
            }
        ],
    };

    pub const Transaction: ResourceType = ResourceType {
        name:"call",
        id:2,
        flags: &[],
        permissions: None,
        fields: &[
            ResourceField {
                key: "from",
                r#type: ResourceFieldType::gid,
                descriptor: "",
                required: true,
                id: 1,
            },
            ResourceField {
                key: "to",
                r#type: ResourceFieldType::gid,
                descriptor: "",
                required: true,
                id: 2,
            },
            ResourceField {
                key: "amt",
                r#type: ResourceFieldType::uint,
                descriptor: "",
                required: true,
                id: 3,
            }
        ],
    };

    pub const Call: ResourceType = ResourceType {
        name:"call",
        id:3,
        flags: &[],
        permissions: None,
        fields: &[
            ResourceField {
                key: "executable",
                r#type: ResourceFieldType::gid,
                descriptor: "",
                required: true,
                id: 1,
            }
        ],
    };
    pub const Executable: ResourceType = ResourceType {
        name:"revoke",
        id:4,
        flags: &[],
        permissions: None,
        fields: &[
            ResourceField {
                key: "bytecode",
                r#type: ResourceFieldType::bytes,
                descriptor: "",
                required: true,
                id: 2,
            }
        ],
    };

    pub const Revoke: ResourceType = ResourceType {
        name:"revoke",
        id:4,
        flags: &[],
        permissions: None,
        fields: &[],
    };

    pub const Generic: ResourceType = ResourceType {
        name:"generic",
        id:100,
        flags: &[],
        permissions: None,
        fields: &[],
    };
    pub const by_type: &[ResourceType] = &[Generic, Key, Record, Transaction, Call];
}