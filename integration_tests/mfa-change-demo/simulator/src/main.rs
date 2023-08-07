
mod simulation;
mod entities;
mod server;

#[tokio::main]
async fn main(){
    println!("This is the recwave actor!");
    simulation::main_loop().await;
}