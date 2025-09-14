#[cfg(test)]
mod tests {
    use crate::protocol::Message;
    use crate::storage::InMemoryStorage;
    use bytes::Bytes;

    #[test]
    fn test_storage_new() {
        let storage = InMemoryStorage::new();
        assert!(storage.get_topics().is_empty());
    }

    #[test]
    fn test_append_single_message() {
        let storage = InMemoryStorage::new();
        let message = Message::new("test message");

        let base_offset = storage
            .append_messages("test-topic", 0, vec![message])
            .expect("Failed to append message");

        assert_eq!(base_offset, 0);

        let topics = storage.get_topics();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0], "test-topic");

        let partitions = storage.get_partitions("test-topic");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], 0);
    }

    #[test]
    fn test_append_multiple_messages() {
        let storage = InMemoryStorage::new();
        let messages = vec![
            Message::new("message 1"),
            Message::new("message 2"),
            Message::new("message 3"),
        ];

        let base_offset = storage
            .append_messages("test-topic", 0, messages)
            .expect("Failed to append messages");

        assert_eq!(base_offset, 0);

        let latest_offset = storage
            .get_latest_offset("test-topic", 0)
            .expect("Failed to get latest offset");
        assert_eq!(latest_offset, 3);
    }

    #[test]
    fn test_fetch_messages() {
        let storage = InMemoryStorage::new();
        let messages = vec![
            Message::new("message 1"),
            Message::new("message 2"),
            Message::new("message 3"),
        ];

        storage
            .append_messages("test-topic", 0, messages)
            .expect("Failed to append messages");

        let fetched = storage
            .fetch_messages("test-topic", 0, 0, 1024)
            .expect("Failed to fetch messages");

        assert_eq!(fetched.len(), 3);
        assert_eq!(fetched[0].0, 0);
        assert_eq!(fetched[0].1.value, Bytes::from("message 1"));
        assert_eq!(fetched[1].0, 1);
        assert_eq!(fetched[1].1.value, Bytes::from("message 2"));
        assert_eq!(fetched[2].0, 2);
        assert_eq!(fetched[2].1.value, Bytes::from("message 3"));
    }

    #[test]
    fn test_fetch_messages_from_offset() {
        let storage = InMemoryStorage::new();
        let messages = vec![
            Message::new("message 1"),
            Message::new("message 2"),
            Message::new("message 3"),
        ];

        storage
            .append_messages("test-topic", 0, messages)
            .expect("Failed to append messages");

        let fetched = storage
            .fetch_messages("test-topic", 0, 1, 1024)
            .expect("Failed to fetch messages");

        assert_eq!(fetched.len(), 2);
        assert_eq!(fetched[0].0, 1);
        assert_eq!(fetched[0].1.value, Bytes::from("message 2"));
        assert_eq!(fetched[1].0, 2);
        assert_eq!(fetched[1].1.value, Bytes::from("message 3"));
    }

    #[test]
    fn test_fetch_nonexistent_topic() {
        let storage = InMemoryStorage::new();

        let fetched = storage
            .fetch_messages("nonexistent-topic", 0, 0, 1024)
            .expect("Failed to fetch from nonexistent topic");

        assert!(fetched.is_empty());
    }

    #[test]
    fn test_fetch_nonexistent_partition() {
        let storage = InMemoryStorage::new();
        let message = Message::new("test message");

        storage
            .append_messages("test-topic", 0, vec![message])
            .expect("Failed to append message");

        let fetched = storage
            .fetch_messages("test-topic", 999, 0, 1024)
            .expect("Failed to fetch from nonexistent partition");

        assert!(fetched.is_empty());
    }

    #[test]
    fn test_multiple_partitions() {
        let storage = InMemoryStorage::new();

        storage
            .append_messages("test-topic", 0, vec![Message::new("partition 0 msg")])
            .expect("Failed to append to partition 0");

        storage
            .append_messages("test-topic", 1, vec![Message::new("partition 1 msg")])
            .expect("Failed to append to partition 1");

        let partitions = storage.get_partitions("test-topic");
        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&0));
        assert!(partitions.contains(&1));

        let p0_messages = storage
            .fetch_messages("test-topic", 0, 0, 1024)
            .expect("Failed to fetch from partition 0");
        assert_eq!(p0_messages.len(), 1);
        assert_eq!(p0_messages[0].1.value, Bytes::from("partition 0 msg"));

        let p1_messages = storage
            .fetch_messages("test-topic", 1, 0, 1024)
            .expect("Failed to fetch from partition 1");
        assert_eq!(p1_messages.len(), 1);
        assert_eq!(p1_messages[0].1.value, Bytes::from("partition 1 msg"));
    }

    #[test]
    fn test_multiple_topics() {
        let storage = InMemoryStorage::new();

        storage
            .append_messages("topic1", 0, vec![Message::new("topic 1 message")])
            .expect("Failed to append to topic1");

        storage
            .append_messages("topic2", 0, vec![Message::new("topic 2 message")])
            .expect("Failed to append to topic2");

        let topics = storage.get_topics();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"topic1".to_string()));
        assert!(topics.contains(&"topic2".to_string()));
    }

    #[test]
    fn test_offset_increments_correctly() {
        let storage = InMemoryStorage::new();

        let base_offset1 = storage
            .append_messages("test-topic", 0, vec![Message::new("msg 1")])
            .expect("Failed to append first batch");
        assert_eq!(base_offset1, 0);

        let base_offset2 = storage
            .append_messages(
                "test-topic",
                0,
                vec![Message::new("msg 2"), Message::new("msg 3")],
            )
            .expect("Failed to append second batch");
        assert_eq!(base_offset2, 1);

        let latest_offset = storage
            .get_latest_offset("test-topic", 0)
            .expect("Failed to get latest offset");
        assert_eq!(latest_offset, 3);
    }

    #[test]
    fn test_get_latest_offset_nonexistent() {
        let storage = InMemoryStorage::new();

        let offset = storage.get_latest_offset("nonexistent", 0);
        assert!(offset.is_none());
    }
}
