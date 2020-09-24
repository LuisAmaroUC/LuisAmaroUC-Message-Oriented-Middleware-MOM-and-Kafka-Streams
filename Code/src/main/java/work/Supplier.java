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

public class Supplier {

    public static void main(String[] args) {


        runConsumer();
    }

    static void runConsumer() {
        Scanner sc = new Scanner(System.in);
        Consumer<String, String> consumer = ConsumerCreator.createConsumer();
        consumer.subscribe(Collections.singletonList("PreorderTopic"));
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());


                System.out.println("Insira  o preço do produto");
                String newPrice = sc.nextLine();
                String value = newPrice + " " + record.value() + " NewValue ";

                runProducer("ShopShipmentsTopic",record.key(),value);

            });

            // commits the offset of record to broker.
            consumer.commitAsync();

        }
        consumer.close();
    }
    static void runProducer(String topicName,String key,String value) {
       // System.out.println("IM IN");
        Producer<String, String> producer = ProducerCreator.createProducer();
        //   for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key,
                value);
        try {
            RecordMetadata metadata = producer.send(record).get();
               /* System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());*/
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
