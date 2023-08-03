mod server;
mod client;
mod broker;

use server::Server;
use client::Client;
use broker::Broker;


fn main() {
    let addr = "127.0.0.1:8080";
    let broker = Broker::new(6);
    let mut server = Server::new(addr, broker);
    let server_thread = std::thread::spawn(move || {
        server.run();
    });
    let client = Client::new(addr);
    client.produce("topic_0", "Hello!");
    client.produce("topic_4", "Bye!");
    let consumed_messages = client.consume("topic_0");
    println!("Consumed messages: {}", consumed_messages);
    let consumed_messages = client.consume("topic_4");
    println!("Consumed messages: {}", consumed_messages);
    server_thread.join().expect("Server thread stopped");
}
