pub struct S3SourceConfig {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AwsStaticCredential {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[cfg(test)]
mod test {
    #[test]
    fn test_asw_conf() {}
}