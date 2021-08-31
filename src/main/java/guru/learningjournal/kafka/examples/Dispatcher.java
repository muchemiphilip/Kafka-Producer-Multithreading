package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.util.Scanner;

public class Dispatcher implements Runnable{
    private static final Logger logger = (Logger) LogManager.getLogger();
    private KafkaProducer<Integer, String> producer;
    private String topicName;
    private String fileLocation;

    public Dispatcher(KafkaProducer<Integer, String> producer, String topicName, String fileLocation) {
        this.producer = producer;
        this.topicName = topicName;
        this.fileLocation = fileLocation;
    }

    @Override
    public void run() {
        logger.info("Start Processing " + fileLocation);
        File file = new File(fileLocation);
        int counter = 0;

        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNext()){
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName,null,line));
                counter++;
            }
            logger.info("Finshed Sending " + counter + "message from" + fileLocation);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
