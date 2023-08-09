mod entities;
mod server;
mod simulation;

#[tokio::main]
async fn main() {
    println!("This is the recwave actor!");
    simulation::main_loop().await;
}
