use std::net::TcpStream;
use std::io::{Read, Write};

pub struct Client {
    address: String,
}

impl Client {
    pub fn new(address: &str) -> Self {
        Client {
            address: address.to_string(),
        }
    }

    pub fn produce(&self, topic: &str, message: &str) {
        let mut stream = TcpStream::connect(&self.address).expect("Failed to connect");
        let request = format!("PRODUCE {} {}", topic, message);
        self.send_request(&mut stream, &request);
        println!("Produced message to topic {}", topic);
    }

    pub fn consume(&self, topic: &str) -> String {
        let mut stream = TcpStream::connect(&self.address).expect("Failed to connect");
        let request = format!("CONSUME {}", topic);
        let response = self.send_request(&mut stream, &request);
        response
    }

    fn send_request(&self, stream: &mut TcpStream, request: &str) -> String {
        stream.write_all(request.as_bytes()).expect("Failed to write to stream");
        let mut buffer = [0; 512];
        stream.read(&mut buffer).expect("Failed to read from stream");
        let response = String::from_utf8_lossy(&buffer[..]).to_string();

        response
    }
}
