use std::collections::{HashMap, VecDeque};

struct Partition {
    pub messages: VecDeque<Message>,
}

impl Partition {
    fn new() -> Self {
        Partition {
            messages: VecDeque::new(),
        }
    }

    fn store_message(&mut self, message: Message) {
        self.messages.push_back(message);
    }

    fn get_messages(&self) -> Vec<Message> {
        self.messages.iter().cloned().collect()
    }
}

pub struct Broker {
    partitions: HashMap<String, Vec<Partition>>, // topic -> partitions
}

impl Broker {
    pub fn new(partition_count: usize) -> Self {
        let mut partitions = HashMap::new();

        for partition_id in 0..partition_count {
            let partition_name = format!("topic_{}", partition_id);
            partitions.insert(partition_name.clone(), vec![Partition::new()]);
        }

        Broker { partitions }
    }

    pub fn store_message(&mut self, message: Message) {
        if let Some(topic_partitions) = self.partitions.get_mut(&message.topic) {
            let partition = topic_partitions.first_mut().expect("No topic found");
            partition.store_message(message);
        } else {
            eprintln!("Topic {} not found", &message.topic);
        }
    }

    pub fn get_messages(&self, topic: &String) -> Vec<Message> {
        if let Some(topic_partitions) = self.partitions.get(topic) {
            let mut all_messages = Vec::new();
            for partition in topic_partitions {
                all_messages.extend_from_slice(&partition.get_messages());
            }
            all_messages
        } else {
            eprintln!("Topic {} not found", topic);
            Vec::new()
        }
    }
}

#[derive(Clone)]
pub struct Message {
    pub topic: String,
    pub payload: String,
}

impl Message {
    pub fn new(topic: &str, payload: &str) -> Self {
        Message {
            topic: topic.to_string(),
            payload: payload.to_string(),
        }
    }
}
