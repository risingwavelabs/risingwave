use arrow_udf::function;
use rust_decimal::Decimal;

#[function("int_42() -> int")]
fn int_42() -> i32 {
    42
}

#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[function("gcd(int, int, int) -> int")]
fn gcd3(a: i32, b: i32, c: i32) -> i32 {
    gcd(gcd(a, b), c)
}

#[function("sleep(int) -> int")]
fn sleep(second: i32) -> i32 {
    std::thread::sleep(std::time::Duration::from_secs(second as u64));
    0
}

#[function("segfault() -> int")]
fn segfault() -> i32 {
    unsafe { (usize::MAX as *const i32).read_volatile() }
}

#[function("oom() -> int")]
fn oom() -> i32 {
    _ = vec![0u8; usize::MAX];
    0
}

#[function("create_file() -> int")]
fn create_file() -> i32 {
    std::fs::File::create("test").unwrap();
    0
}

#[function("length(varchar) -> int")]
#[function("length(bytea) -> int")]
fn length(s: impl AsRef<[u8]>) -> i32 {
    s.as_ref().len() as i32
}

#[function("extract_tcp_info(bytea) -> struct<src_addr:varchar,dst_addr:varchar,src_port:smallint,dst_port:smallint>")]
fn extract_tcp_info(tcp_packet: &[u8]) -> (String, String, i16, i16) {
    let src_addr = std::net::Ipv4Addr::from(<[u8; 4]>::try_from(&tcp_packet[12..16]).unwrap());
    let dst_addr = std::net::Ipv4Addr::from(<[u8; 4]>::try_from(&tcp_packet[16..20]).unwrap());
    let src_port = u16::from_be_bytes(<[u8; 2]>::try_from(&tcp_packet[20..22]).unwrap());
    let dst_port = u16::from_be_bytes(<[u8; 2]>::try_from(&tcp_packet[22..24]).unwrap());
    (
        src_addr.to_string(),
        dst_addr.to_string(),
        src_port as i16,
        dst_port as i16,
    )
}

#[function("decimal_add(decimal, decimal) -> decimal")]
fn decimal_add(a: Decimal, b: Decimal) -> Decimal {
    a + b
}

#[function("jsonb_access(json, int) -> json")]
fn jsonb_access(json: serde_json::Value, index: i32) -> Option<serde_json::Value> {
    json.get(index as usize).cloned()
}

#[function("series(int) -> setof int")]
fn series(n: i32) -> impl Iterator<Item = i32> {
    0..n
}
