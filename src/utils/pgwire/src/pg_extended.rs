use bytes::{Bytes, BytesMut};
use regex::Regex;

use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};

// NOTE : Code dundancy , may need to modify later
fn cstr_to_str(b: &Bytes) -> &str {
    let without_null = if b.last() == Some(&0) {
        &b[..b.len() - 1]
    } else {
        &b[..]
    };
    std::str::from_utf8(without_null).unwrap()
}

pub struct pg_statement {
    name: String,
    query_string: Bytes,
    type_description: Vec<TypeOid>,
    row_description: Vec<PgFieldDescriptor>,
}

impl pg_statement {
    pub fn new(
        name: String,
        query_string: Bytes,
        type_description: Vec<TypeOid>,
        row_description: Vec<PgFieldDescriptor>,
    ) -> Self {
        // LIMIT: the number of type_description should equal to the number of generic parameter
        {
            let parameter_parttern = Regex::new(r"\$[0-9][0-9]*").unwrap();
            let s = cstr_to_str(&query_string);
            let count = parameter_parttern.find_iter(s).count();
            assert_eq!(count, type_description.len());
        }
        pg_statement {
            name,
            query_string,
            type_description,
            row_description,
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_type_desc(&self) -> Vec<TypeOid> {
        self.type_description.clone()
    }

    pub fn get_row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_description.clone()
    }

    pub fn instance(&self, name: String, params: &Vec<Bytes>) -> pg_portal {
        let statement = cstr_to_str(&self.query_string).to_owned();
        // Step 1 Identify all the $n
        let parameter_parttern = Regex::new(r"\$[0-9][0-9]*").unwrap();
        let mut generic_params: Vec<&str> = parameter_parttern
            .find_iter(statement.as_str())
            .map(|mat| mat.as_str())
            .collect();
        // Step 2 Sort by len (From large to small )
        generic_params.sort_by(|a, b| {
            let a_num: i32 = a.trim_start_matches("$").parse().unwrap();
            let b_num: i32 = b.trim_start_matches("$").parse().unwrap();
            b_num.cmp(&a_num)
        });
        // LTIMIT: may need modify later;
        {
            assert!(generic_params.starts_with(&[format!("${}", generic_params.len()).as_str()]));
            assert_eq!(generic_params.len(), params.len());
        }
        // Step 3 replace top-down
        let mut tmp = statement.clone();
        for i in 0..generic_params.len() {
            let parttern = Regex::new(format!(r"\{}", generic_params[i]).as_str()).unwrap();
            let param = cstr_to_str(&params[i]);
            tmp = parttern.replace_all(tmp.as_str(), param).to_string();
        }
        // Step 4 Create a new portal
        pg_portal {
            name: name,
            query_string: Bytes::from(tmp.to_owned()),
        }
    }
}

pub struct pg_portal {
    name: String,
    query_string: Bytes,
}

impl pg_portal {
    pub fn new(name: String, query_string: Bytes) -> Self {
        pg_portal { name, query_string }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_query_string(&self) -> Bytes {
        self.query_string.clone()
    }
}
