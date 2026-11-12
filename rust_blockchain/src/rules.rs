

pub struct Field<'a>{
    pub required: bool,
    pub name: &'a str,
    pub r#type: &'a str,
    pub rules: &'a [&'a [&'a str]]
}

pub struct Rules<'a>{
    pub name: &'a str,
    pub fields: &'a [Field<'a>]
}


pub const SIGNATURE:Rules = Rules{
    name: "SIGNATURE",
    fields: &[
        Field{
            required: true,
            r#type: "grid",
            name: "key",
            rules: &[
                &["INCLUDE", "PARENT", "KEY"]
            ]
        },
    ]
};

pub const LROOT:Rules = Rules{
    name: "SIGNATURE",
    fields: &[
        Field{
            required: true,
            r#type: "grid",
            name: "key",
            rules: &[
                &["INCLUDE", "PARENT", "KEY"]
            ]
        },
    ]
};

pub const REVOKE:Rules = Rules{
    name: "REVOKE",
    fields: &[
        Field{
            required: true,
            r#type: "grids",
            name: "grids",
            rules: &[]
        },
        Field{
            required: true,
            r#type: "grids",
            name: "key",
            rules: &[]
        },
        Field{
            required: true,
            r#type: "grids",
            name: "signature",
            rules: &[]
        }
    ]
};