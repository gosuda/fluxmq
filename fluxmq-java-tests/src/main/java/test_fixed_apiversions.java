import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class test_fixed_apiversions {
    public static void main(String[] args) throws Exception {
        System.out.println("🧪 Testing Fixed ApiVersions Response Format");
        System.out.println("============================================");
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");  // Changed to 9092
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("max.in.flight.requests.per.connection", 1);
        props.put("request.timeout.ms", 5000);
        
        System.out.println("Creating producer with corrected port 9092...");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("✅ Producer created successfully!");
            System.out.println("Sending test message...");
            
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "Fixed ApiVersions test message");
            producer.send(record).get();
            
            System.out.println("✅ Message sent successfully! ApiVersions fix working!");
            
        } catch (Exception e) {
            System.out.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}