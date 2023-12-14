use std::time::Duration;

async fn h() {}

async fn g() {
    h().await
}

async fn f() {
    for _i in 0..10 {
        g().await;
    }
}

struct St;

impl St {
    async fn next(&mut self) -> Option<i32> {
        Some(0)
    }
}

async fn f2() {
    let mut s = St;

    // Don't lint
    while let Some(_) = s.next().await {}

    // Should lint due go g().await
    while let Some(_) = s.next().await {
        g().await;
    }

    // Don't lint, we add sleep in whitelist.
    while let Some(_) = s.next().await {
        tokio::time::sleep(Duration::ZERO).await;
    }
}

#[tokio::main]
async fn main() {
    let _ = f().await;
    let _ = f2().await;
}
