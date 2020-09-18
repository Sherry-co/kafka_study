package kafkaacl;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class KafkaAclConsumer {
    Logger logger = LoggerFactory.getLogger(KafkaAclConsumer.class);
    private static KafkaConsumer<String, String> consumer;
    private static Properties kfkProperties;

    static {
        kfkProperties = new Properties();
        kfkProperties.put("bootstrap.servers", "localhost:9092");
        kfkProperties.put("security.protocol", "SASL_PLAINTEXT");
        kfkProperties.put("sasl.mechanism", "PLAIN");
        kfkProperties.put("group.id", "1");
        kfkProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kfkProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        System.setProperty("java.security.auth.login.config",
                "D:\\kafka\\src\\main\\resources\\kafka_client_jaas.conf"); // 环境变量添加，需要输入配置文件的路径
    }

    private static void generalConsumerMessageAutoCommit() {
        kfkProperties.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<>(kfkProperties);
        String topic = "topic";
        TopicPartition partition = new TopicPartition(topic, 0);
        List<TopicPartition> lists = new ArrayList<TopicPartition>();
        lists.add(partition);
        consumer.assign(lists);
        consumer.seekToBeginning(lists);


        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(8000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.timestamp() + "," + record.topic() + "," + record.partition() + "," + record.offset() + " " + record.key() + "," + record.value());
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void generalConsumerMessageManualCommitSync() {
        kfkProperties.put("enable.auto.commit", false);
        consumer = new KafkaConsumer<>(kfkProperties);
        consumer.subscribe(Collections.singletonList("topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(80);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.timestamp() + "," + record.topic() + "," + record.partition() + "," + record.offset() + " " + record.key() + "," + record.value());
            }
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                System.out.println("commit failed msg" + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaAclConsumer.generalConsumerMessageAutoCommit();
    }
}


