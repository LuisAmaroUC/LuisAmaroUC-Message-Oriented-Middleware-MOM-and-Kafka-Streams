package work;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class Owner {

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        System.out.println("Insira o nome do Produto que quer pedir");
        String key = sc.nextLine();
        System.out.println("Insira a quantidade");
        String value = sc.nextLine();

        String newKey = 1 +" " + key;
        runProducer("PreorderTopic",newKey, value);
    }


    static void runProducer(String topicName,String key,String value) {
        Producer<String, String> producer = ProducerCreator.createProducer();

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key,
                value);
        try {
            RecordMetadata metadata = producer.send(record).get();
        }
        catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
        catch (InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
        // }
    }

}
