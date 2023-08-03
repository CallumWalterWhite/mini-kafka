use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use crate::broker::{Broker, Message};

pub struct Server {
    address: String,
    broker: Broker,
}

impl Server {
    pub fn new(address: &str, broker: Broker) -> Self {
        Server {
            address: address.to_string(),
            broker,
        }
    }

    pub fn run(&mut self) {
        let listener = TcpListener::bind(&self.address).expect("Failed to bind");
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    self.handle_connection(stream);
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }
    }

    fn handle_connection(&mut self, mut stream: TcpStream) {
        let mut buffer = [0; 512];
        let n = stream.read(&mut buffer).expect("Failed to read from stream");
        let request: String = format!("{}", String::from_utf8_lossy(&buffer[0..n]).trim());
        let response = self.process_request(&request);
        stream.write_all(response.as_bytes()).expect("Failed to write to stream");
    }

    fn process_request(&mut self, request: &str) -> String {
        let mut response = String::new();
    
        if request.starts_with("PRODUCE") {
            let parts: Vec<&str> = request.split_whitespace().collect();
            if parts.len() >= 3 {
                let topic = parts[1];
                let message_payload = parts[2];
                let message = Message::new(topic, message_payload); 
                self.broker.store_message(message);
    
                response = format!("Produced message to topic {}", topic);
            }
        } else if request.starts_with("CONSUME") {
            let parts: Vec<&str> = request.split_whitespace().collect();
            if parts.len() >= 2 {
                let topic: &String = &String::from(parts[1].trim_end());
                let messages = self.broker.get_messages(topic);
                let consumed_messages: Vec<&String> = messages.iter().map(|msg| &msg.payload).collect();
    
                response = format!("Consumed messages from topic: {:?}", consumed_messages);
            }
        } else {
            response = "Invalid request".to_string();
        }
    
        response
    }
    
}
