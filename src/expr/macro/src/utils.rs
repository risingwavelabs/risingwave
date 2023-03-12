/// Convert a string from snake_case to CamelCase.
pub fn to_camel_case(input: &str) -> String {
    input
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first_char) => {
                    format!("{}{}", first_char.to_uppercase(), chars.as_str())
                }
            }
        })
        .collect()
}
