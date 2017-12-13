package es.upm.master.zookeeper.kafkaCode;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Properties;

public class Kafka {

    private Stat stat;
    public ZooKeeper zoo;
    private String enroll = "/System/Request/Enroll/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";
    private String online = "/System/Online/";
    private String queue = "/System/Queue/";
    private String backup ="/System/Backup/";



    public static void main(String[] args) throws Exception {

        String name = "Santiago";
        String msg = "Mensaje 1";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);
        String topic = name.toString();
        int partition = 0;
        String key = "From: " + name.toString();
        String value = msg.toString();
        prod.send(new ProducerRecord<String, String>(topic,partition,key, value));
        prod.close();




    }




}