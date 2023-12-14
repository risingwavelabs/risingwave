use arrow_udf::function;

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
