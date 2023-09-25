package org.opendc.oda.experimentrunner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;

public final class ODAExperimentKafkaProducer {
    private static final String TOPIC = "test-consumer-group-1";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static Producer<String, byte[]> producer;
    private static Properties props;
    public static Producer<String, byte[]> getKafkaProducerInstance() {
//        System.out.println("Kafka ClusterID for local environment - Fbfi0xWtSg208OMugpj5lg");
//        System.out.println("Set storage location - ./bin/kafka-storage.sh format -t Fbfi0xWtSg208OMugpj5lg -c ./config/kraft/server.properties");
//        System.out.println("Start Kafka - ./bin/kafka-server-start.sh ./config/kraft/server.properties");
//        System.out.println("----------------------------------------------------------------------------------------");
        // Create configuration options for our producer and initialize a new producer
        if(props == null) {
            props = new Properties();
            props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
            // We configure the serializer to describe the format in which we want to produce data into
            // our Kafka cluster
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        }
        // Since we need to close our producer, we can use the try-with-resources statement to
        // create a new producer
        try {
            if(producer == null) {
                producer = new KafkaProducer<>(props);
                System.out.println("Created kafka producer instance for the first time!");
            }
            return producer;
        } catch (Exception e) {
            System.out.println("Could not start producer: " + e);
            return null;
        }
    }


}

//public class ODAExperimentKafkaProducer<String, Byte[]> extends KafkaProducer {
//
//    private ODAExperimentKafkaProducer<String, Byte[]>(){}
//    // singleton quick help - https://www.digitalocean.com/community/tutorials/java-singleton-design-pattern-best-practices-examples#5-bill-pugh-singleton-implementation
//    private static class SingletonHelper {
//        //
//        private static final String TOPIC = "test";
//        private static final String BOOTSTRAP_SERVERS = "localhost:9092";
//        private static Producer<String, byte[]> producer;
//        private static void initialiseKafkaProducer() {
//            Properties properties = new Properties();
//            properties.put("bootstrap.servers", BOOTSTRAP_SERVERS); // Replace with your Kafka broker(s)
//            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//            properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
//            final ODAExperimentKafkaProducer INSTANCE = new ODAExperimentKafkaProducer();
//        }
//
//            private static void releaseKafkaProducer(){
//                if(producer != null)
//                    producer.close();
//            }
//    }
//
//    public static ODAExperimentKafkaProducer getInstance() {
//        return SingletonHelper.INSTANCE;
//    }
//}
