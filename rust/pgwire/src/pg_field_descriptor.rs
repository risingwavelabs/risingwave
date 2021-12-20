/// Port from PgFieldDescriptor.java
#[derive(Clone)]
pub struct PgFieldDescriptor {
    name: String,
    table_oid: i32,
    col_attr_num: i16,

    // NOTE: Static code for data type. To see the oid of a specific type in Postgres,
    // use the following command:
    //   SELECT oid FROM pg_type WHERE typname = 'int4';
    type_oid: i32,

    type_len: i16,
    type_modifier: i32,
    format_code: i16,
}

impl PgFieldDescriptor {
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_table_oid(&self) -> i32 {
        self.table_oid
    }

    pub fn get_col_attr_num(&self) -> i16 {
        self.col_attr_num
    }

    pub fn get_type_oid(&self) -> i32 {
        self.type_oid
    }

    pub fn get_type_len(&self) -> i16 {
        self.type_len
    }

    pub fn get_type_modifier(&self) -> i32 {
        self.type_modifier
    }

    pub fn get_format_code(&self) -> i16 {
        self.format_code
    }
}
