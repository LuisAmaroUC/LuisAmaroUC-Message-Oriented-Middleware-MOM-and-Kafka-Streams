


package work;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;




public class Shop extends Thread{


    static Connection crunchifyConn = null;
    static PreparedStatement crunchifyPrepareStat = null;

    public static void main(String[] args) {
        //try {
            log("-------- Simple Crunchify Tutorial on how to make JDBC connection to MySQL DB locally on macOS ------------");
            makeJDBCConnection();

          /*  log("\n---------- Adding company 'Crunchify LLC' to DB ----------");
           addDataToDB("Batatas",2.1);
            addDataToDB("Cenouras", 5.1);
            //addDataToDB("Apple Inc.", "Cupertino, CA, US", 30000, "http://apple.com");

            log("\n---------- Let's get Data from DB ----------");
            getDataFromDB();

            crunchifyPrepareStat.close();
            crunchifyConn.close(); // connection close


        } catch (SQLException e) {

            e.printStackTrace();
        }*/

         runConsumer();

    }

    static void runConsumer() {
        Consumer<String, String> consumer = ConsumerCreator.createConsumer();
        consumer.subscribe(Pattern.compile("Shop.*"));
        int noMessageFound = 0;


        while (true) {


            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            for (TopicPartition partition : consumerRecords.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = consumerRecords.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {

                    //AQUI TEMOS DE DISTINGUIR A PRIMEIRA ENTREGA DAS OUTRAS
                    //TEMOS DE DISTINGUIR OS PEDIDOS DO OWNER DOS PEDIDOS DO CUSTOMER(1-OWNER | 0 - CUSTOMER)
                    if(record.topic().equals("ShopShipmentsTopic")){
                       // System.out.println("KEY SHOPSHIP"+record.key());
                       //
                        // System.out.println("VALOR SHOPSHIP"+record.value());
                        String parts[] = record.value().split(" ");//SEPARAR A QUANTIDADE DO PREÇO
                        String flag[] = record.key().split(" ");//DISTINGUIR O PEDIDO DO OWNER DO PEDIDO DO CUSTOMER

                    //    System.out.println("IM HERE");
                        //se for owner fazer 30% dos preços e atualizar a base de dados
                        if(Integer.parseInt(flag[0]) == 1) {
                           addDataToDBOwner(flag[1],Double.parseDouble(parts[0]), Integer.parseInt(parts[1]));//SE FOR OWNER BASTA ADICIONAR A BD
                        }
                        else if(Integer.parseInt(flag[0]) == 0){
                            //se preço iguais encomenta satisfeita com sucesso caso contrario adicionar produto a DB e atualizapreços
                           addDataToDBCustomer(flag[1],parts[2],Double.parseDouble(parts[0]), Integer.parseInt(parts[1]));//SE FOR OWNER BASTA ADICIONAR A BD
                           // runProducer(flag[2],flag[1],record.value());
                        }
                        else System.out.println("BAD FLAG, OWNER != CUSTOMER");

                       // System.out.println("INSERIR PRODUTOS NA BASE DE DADOS");


                    }else if(record.topic().equals("ShopPurchasesTopic")){
                       //System.out.println("VER SE O ITEM EXISTE NA BASE DE DADOS");
                        //System.out.println(record.key());
                        String topic[] = record.key().split(" ");
                        //System.out.println("TOPICO 0"+topic[0]+"TOPICO 1"+topic[1]);

                       if(topic[0].equals("seeDB")){getDataFromDB(topic[1]);}
                        else {
                            String id[] = record.value().split(" ");
                            String topicName = "MyReplyTopic" + id[2];
                            String parts2[] = record.value().split(" ");

                            getItemsFromDB(id[2],topic[0],Double.parseDouble(parts2[0]),Integer.parseInt(parts2[1]));



                         //   System.out.println("VOU ENVIAR PARA ESTE TOPICO"+topicName);
                           // runProducer(topicName,"","->Compra Bem Sucedida, comprou:"+record.key());
                        }
                    }else
                        System.out.println("TOPICO DESCONHECIDO");
                }
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker{.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }

            // commits the offset of record to broker.
            consumer.commitAsync();

        }
        consumer.close();
    }

    static void runProducer(String topicName,String key,String value) {
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



        private static void makeJDBCConnection() {

            try {
                Class.forName("com.mysql.jdbc.Driver");
                log("Congrats - Seems your MySQL JDBC Driver Registered!");
            } catch (ClassNotFoundException e) {
                log("Sorry, couldn't found JDBC driver. Make sure you have added JDBC Maven Dependency Correctly");
                e.printStackTrace();
                return;
            }

            try {
                // DriverManager: The basic service for managing a set of JDBC drivers.
                crunchifyConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/teste?useTimezone=true&serverTimezone=UTC&useSSL=false", "root", "root");
                if (crunchifyConn != null) {
                    log("Connection Successful! Enjoy. Now it's time to push data");
                } else {
                    log("Failed to make connection!");
                }
            } catch (SQLException e) {
                log("MySQL Connection Failed!");
                e.printStackTrace();
                return;
            }

        }

        private static void addDataToDB(String name, double price, int quantidade) {

            try {
                String insertQueryStatement = "INSERT  INTO  item  (Name, Price,Quantidade,InitialValue)   VALUES  (?,?,?,?)";

                crunchifyPrepareStat = crunchifyConn.prepareStatement(insertQueryStatement);
                crunchifyPrepareStat.setString(1, name);
                crunchifyPrepareStat.setDouble(2, price);
                crunchifyPrepareStat.setInt(3,quantidade);
                crunchifyPrepareStat.setInt(4,quantidade);


                // execute insert SQL statement
                crunchifyPrepareStat.executeUpdate();
                log(name + " added successfully");
            } catch (

                    SQLException e) {
                e.printStackTrace();
            }
        }

    private static void addDataToDBOwner(String name, double price, int quantidade) {

        //30% dos novos itens inseridos
        double newPrice = price + price*0.3;



        try {


            String selectQueryStatement = "SELECT i.quantidade FROM item i WHERE i.name = ?";
            crunchifyPrepareStat = crunchifyConn.prepareStatement(selectQueryStatement);
            crunchifyPrepareStat.setString(1, name);

            ResultSet rs = crunchifyPrepareStat.executeQuery();
            //SE O PRODUTO JA EXISTIR INSERIR NOVO PREÇO E SOMA QUANTIDADE
            if(rs.next()){

                int oldQuantidade = rs.getInt("Quantidade");
                //ATUALIZAR OS ITEMS EXISTENTES
                String updateQueryStatement = "UPDATE item i SET i.price = ? ,i.quantidade = ? WHERE i.name LIKE ? ";
                crunchifyPrepareStat = crunchifyConn.prepareStatement(updateQueryStatement);
                crunchifyPrepareStat.setDouble(1, newPrice);
                crunchifyPrepareStat.setInt(2, quantidade+oldQuantidade);
                crunchifyPrepareStat.setString(3, name);
                // execute update SQL statement
                crunchifyPrepareStat.executeUpdate();


            }else {
                //INSERIR NOVO PRODUTO NA BASE DE DADOS
                String insertQueryStatement = "INSERT  INTO  item  (Name, Price,Quantidade,InitialValue)   VALUES  (?,?,?,?)";
                crunchifyPrepareStat = crunchifyConn.prepareStatement(insertQueryStatement);
                crunchifyPrepareStat.setString(1, name);
                crunchifyPrepareStat.setDouble(2, newPrice);
                crunchifyPrepareStat.setInt(3, quantidade);
                crunchifyPrepareStat.setInt(4, quantidade);
            }



            // execute insert SQL statement
            crunchifyPrepareStat.executeUpdate();
            log(name + " added successfully");
        } catch (

                SQLException e) {
            e.printStackTrace();
        }
    }
    private static void addDataToDBCustomer(String name,String topic, double price, int quantidade) {

        //30% dos novos itens inseridos
        double newPrice = price + price*0.3;

        String topicName = "MyReplyTopic" + topic;

        try {


            String selectQueryStatement = "SELECT i.price FROM item i WHERE i.name = ?";
            crunchifyPrepareStat = crunchifyConn.prepareStatement(selectQueryStatement);
            crunchifyPrepareStat.setString(1, name);

            ResultSet rs = crunchifyPrepareStat.executeQuery();

            if(rs.next()){

                double oldPrice = rs.getDouble("Price");
                if(oldPrice == price + price*0.3){
                    runProducer(topicName,name,"Encomenta Sucesso");
                }
                else {
                    runProducer(topicName,name,"Encomenta não pode ser satisfeita");
                    addDataToDBOwner(name, price, quantidade);
                }

            }else{
                runProducer(topic,name,"Produto não existe");
            }


            log(name + " added successfully");
        } catch (

                SQLException e) {
            e.printStackTrace();
        }
    }
        private static void getDataFromDB(String topic) {
            boolean empty = true;

            try {
                // MySQL Select Query Tutorial
                String getQueryStatement = "SELECT * FROM item";

                crunchifyPrepareStat = crunchifyConn.prepareStatement(getQueryStatement);

                // Execute the Query, and get a java ResultSet
                ResultSet rs = crunchifyPrepareStat.executeQuery();

                // Let's iterate through the java ResultSet

                String topicName = "MyReplyTopic" + topic;
                while (rs.next()) {
                    empty = false;
                    String name = rs.getString("Name");
                    double price = rs.getDouble("Price");
                    int quantidade = rs.getInt("Quantidade");

                    // Simply Print the results
                  //  System.out.format("%s, %2f,%d\n", name, price,quantidade);
                    String value = Double.toString(price) + " " + Integer.toString(quantidade)+ " " +topic + " ";
                    runProducer(topicName,name,value);
                }
                if(empty)  runProducer(topicName,"seeDB","Database Empty");

            } catch (

                    SQLException e) {
                e.printStackTrace();
            }

        }
    private static void getItemsFromDB(String topic, String name,  double price, int quantidade) {
        boolean empty = true;

        try {
            // MySQL Select Query Tutorial
            String getQueryStatement = "SELECT * FROM item i WHERE i.name LIKE ? AND ABS(i.price-?) < 0.00001";


            crunchifyPrepareStat = crunchifyConn.prepareStatement(getQueryStatement);
            crunchifyPrepareStat.setString(1, name);
            crunchifyPrepareStat.setDouble(2, price);
           // crunchifyPrepareStat.setInt(3, quantidade);

            // Execute the Query, and get a java ResultSet
            ResultSet rs = crunchifyPrepareStat.executeQuery();

            // Let's iterate through the java ResultSet

            String topicName = "MyReplyTopic" + topic;
           // System.out.println("NEW TOPIC NAME"+ topicName);
            while (rs.next()) {
              //  System.out.format("%s, %2f,%d\n", name, price,quantidade);
                empty = false;
                int idItem = rs.getInt("idItem");
                String name1 = rs.getString("Name");
                double price1 = rs.getDouble("Price");
                int quantidade1 = rs.getInt("Quantidade");
                int initialValue = rs.getInt("InitialValue");

                //VERIFICAR SE OS TENS DA BASE DE DADOS CONSEGEM PREENCHER A ENCOMENDA
                if((quantidade1-quantidade) >= 0.25*initialValue){

                    String updateQueryStatement = "UPDATE item i SET i.quantidade = ?  WHERE i.idItem = ? ";
                    crunchifyPrepareStat = crunchifyConn.prepareStatement(updateQueryStatement);
                    crunchifyPrepareStat.setInt(2, idItem);
                    crunchifyPrepareStat.setInt(1, (quantidade1-quantidade));
                    // execute update SQL statement
                    crunchifyPrepareStat.executeUpdate();

                    String value = Double.toString(price1) + " " + Integer.toString(quantidade1);
                    runProducer(topicName,name1,value);

                }
                else{
                    String newName = 0 + " "+ name;
                    //System.out.println("NEW NAME CUSTOMER PREORDER"+name);
                    String newValue = Integer.toString(quantidade) + " " +topic + " NewValue ";
                    runProducer("PreorderTopic",newName,newValue);
                    //runProducer(topicName,name1,"Pedido Efetuado com sucesso");
                }

                // Simply Print the results
                //

            }

            //SE A BASE DE DADOS ESTA VAZIA OU N TIVER O PREÇO CERTO A ENCOMENDA VAI SER REJEITADA
            if(empty)  {
                runProducer(topicName,"","Encomenda Rejeitada");
            }

        } catch (

                SQLException e) {
            e.printStackTrace();
        }

    }

        // Simple log utility
        private static void log(String string) {
            System.out.println(string);

        }

}