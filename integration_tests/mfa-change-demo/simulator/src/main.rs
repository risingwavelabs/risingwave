mod entities;
mod server_pb;
mod simulation;
mod entities_taxi;

#[tokio::main]
async fn main() {
    println!("This is the recwave actor!");
    simulation::main_loop().await;
}
