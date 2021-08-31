package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DispatcherDemo {
    private static final Logger logger =  LogManager.getLogger();

    public static void main(String[] args) {

        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            properties.load(inputStream);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        KafkaProducer<Integer,String> producer = new KafkaProducer<>(properties);
        Thread[] dispatchers = new Thread[AppConfigs.eventFiles.length];
        logger.info("Starting Dispatcher Thread.....");
        for (int i=0 ; i < AppConfigs.eventFiles.length; i++){
            dispatchers[i] = new Thread(new Dispatcher(producer,AppConfigs.topicName,AppConfigs.eventFiles[i]));
            dispatchers[i].start();
        }
        try {
            for (Thread t : dispatchers) t.join();
        } catch (InterruptedException e){
            logger.error("Main Thread Interrupted");
        }finally {
            producer.close();
            logger.info("Closing Producer and Finished Dispatcher Demo");
        }

    }
}
