use std::path::Path;
use std::{thread, time};

use coredump;
use rlimit::{getrlimit, Resource};

fn main() {
    if !Path::new("/var/coredump").is_dir() {
        println!("/var/coredump does not exist")
    } else {
        println!("ready to write core files to /var/coredump")
    }

    let (x, _) = getrlimit(Resource::CORE).unwrap();
    println!("Core file limit: {}", x);

    coredump::register_panic_handler().expect("unable to register panic handler");
    loop {
        let time = time::SystemTime::now();
        let epoch = time.duration_since(time::SystemTime::UNIX_EPOCH).unwrap();
        let not_really_rand = epoch.as_millis() % 10;
        println!("not really rand was {}", not_really_rand);
        if not_really_rand == 0 {
            panic!("randomly panicking here")
        }
        thread::sleep(time::Duration::from_secs(1));
    }
}
