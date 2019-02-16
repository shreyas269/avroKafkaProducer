package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.constants.Constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;


public class AvroProducer {

    public static Properties properties = new Properties();
    private static String path;

    private static void loadProperties(){
        properties.put("bootstrap.servers", properties.getProperty(Constants.BOOTSTRAP_SERVERS, "localhost:9092"));
        properties.put("zookeeper.hosts", properties.getProperty(Constants.ZOOKEEPER_HOSTS, "localhost:2181"));
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("group.id", properties.getProperty(Constants.CONSUMER_GROUP, "kafkaProducer"));
        properties.put("key.serializer", properties.getProperty(Constants.KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));
        properties.put("value.serializer", properties.getProperty(Constants.VALUE_SERIALIZER, "org.apache.kafka.common.serialization.ByteArraySerializer"));
    }

    private static void publishMessages(Producer<String, byte[]> producer) throws IOException{
        int msgNum = 0;
        int msgCount = Integer.parseInt(properties.getProperty(Constants.NUM_MESSAGES));
        String topicName = properties.getProperty(Constants.TOPIC_NAME);
        path = properties.getProperty(Constants.MESSAGE_PATH);

        System.out.println("Publishing messages in Topic: " + topicName);

        while (msgNum < msgCount) {

            byte[] data = Files.readAllBytes((new File(path)).toPath());
            producer.send(new ProducerRecord<>(topicName, null, data));
            msgNum++;
        }
        System.out.println("Messages sent : " + msgCount);
        producer.close();
    }

    private static Properties getProperties(final InputStream is) throws IOException{
        Properties props = new Properties();
        props.load(is);
        return props;
    }

    public static void main(String[] args) throws IOException {

        System.out.println("--Started avro Kafka Producer--");

        try(InputStream is = new FileInputStream(args[0])){
            properties = getProperties(is);
        }

        loadProperties();

        Producer<String, byte[]> producer = new KafkaProducer<>(properties);

        publishMessages(producer);

    }
}