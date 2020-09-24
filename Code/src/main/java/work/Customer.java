package work;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Customer extends Thread{
    BigInteger b = new BigInteger(256,new Random());
    public static void main(String[] args) {

        (new Thread(new Customer())).start();



    }
    public void run(){
        Scanner sc = new Scanner(System.in);
        System.out.println("1-> Ver BD || 2-> Fazer Pedido");
        int flag = sc.nextInt();
      /*  System.out.println("Insira ID do cliente");
        int id = sc.nextInt();*/
       // System.out.println(b);
        runProducer(flag, b);
    }
    static void runProducer(int flag, BigInteger id) {


        Producer<String, String> producer = ProducerCreator.createProducer();
        ProducerRecord<String, String> record = null;
        ProducerRecord<String,String> record2 = null;
        //System.out.println(producer.metrics().values());

        //for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
        /*for() GERAR INIDICES AUTOMATICOS*/

            String key0 = "";
            String value0 = "";
            String key1 = "";
            String value1 = "";

            List<String> key = new ArrayList<>();
            List<String> value = new ArrayList<>();

            if (flag == 1) {
                key0 += "seeDB" + " " + id;
                record = new ProducerRecord<String, String>("ShopPurchasesTopic", key0, "");

                try {
                    RecordMetadata metadata = producer.send(record).get();
                    // metadata = producer.send(record2).get();

                    /*System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                            + " with offset " + metadata.offset());*/
                } catch (ExecutionException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                } catch (InterruptedException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }
            } else if (flag == 2) {
                key0 = "Batatas";
                value0= "13" + " " + "10" + " " + id + " ";
                key1 = "Cenouras";
                value1 = "13" + " " + "10" + " " + id + " ";

                key.add(key0); key.add(key1);
                value.add(value0); value.add(value1);

                for(int i = 0; i <2; i++){

                    record = new ProducerRecord<String, String>("ShopPurchasesTopic", key.get(i), value.get(i));
                    try {
                        RecordMetadata metadata = producer.send(record).get();
                        // metadata = producer.send(record2).get();

                    /*System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                            + " with offset " + metadata.offset());*/
                    } catch (ExecutionException e) {
                        System.out.println("Error in sending record");
                        System.out.println(e);
                    } catch (InterruptedException e) {
                        System.out.println("Error in sending record");
                        System.out.println(e);
                    }
                }



            } else System.out.println("BAD FLAG");

        runConsumer(id);





        }
    static void runConsumer(BigInteger id) {
        String topic = "MyReplyTopic" + id;
      //  System.out.println("ESTOU A LER DESTE TOPICO"+topic);
        Consumer<String, String> consumer = ConsumerCreator.createConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        int noMessageFound = 0;
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1);
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
                System.out.println("Pedido de" + record.key() + " foi bem sucedido");
                //System.out.println("Record value " + record.value());
                //System.out.println("Record partition " + record.partition());
                //System.out.println("Record offset " + record.offset());


            });
            // commits the offset of record to broker.
            consumer.commitAsync();

        }

        consumer.close();
        (new Thread(new Customer())).start();
    }
    //}
}
