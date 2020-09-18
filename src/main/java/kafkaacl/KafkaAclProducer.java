package kafkaacl;

import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaAclProducer {
    private static KafkaProducer<String, String> producer = null;
    Logger logger = (Logger) LoggerFactory.getLogger(KafkaAclProducer.class);

    static {
        Properties props = initConfig();
        producer = new KafkaProducer<String, String>(props);
    }

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        System.setProperty("java.security.auth.login.config",
                "D:\\kafka\\src\\main\\resources\\kafka_client_jaas.conf"); // 环境变量添加，需要输入配置文件的路径
        return props;
    }

    public static void main(String[] args) throws InterruptedException {
        //消息实体
        ProducerRecord<String, String> record = null;
        System.out.println("程序开始");
        for (int i = 0; i < 1000; i++) {
            record = new ProducerRecord<String, String>("topic", "value" + i);
            //发送消息
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null != e) {
                        e.printStackTrace();
                        System.out.println("message = " + e.getMessage());
                    } else {
                        System.out.println(String.format("offset:%s,partition:%s", recordMetadata.offset(), recordMetadata.partition()));
                    }
                }
            });
        }
        producer.close();
    }
}
