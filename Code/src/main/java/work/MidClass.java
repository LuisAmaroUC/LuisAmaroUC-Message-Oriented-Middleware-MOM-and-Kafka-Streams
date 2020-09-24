package work;

import java.util.ArrayList;
import java.util.List;

import javax.mail.internet.InternetAddress;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;


@Path("/students")
public class MidClass {

   // private static final String tablename = "tableTester";

   @Path("xpto")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Long getAllItems() throws IOException, InterruptedException {

        List<String> newList= new ArrayList<String>();
        String topicName = "ShopPurchasesTopic";


        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(topicName);

        KTable<String, Long> countlines = lines.groupByKey().count(Materialized.as("first"));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();



        System.out.println("Press enter when ready...");
        System.in.read();

        ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store("first", QueryableStoreTypes.<String,Long>keyValueStore());
        String key = "Batatas";
        String key2 = "Cenouras";
        Long result = keyValueStore.get(key);

        result += keyValueStore.get(key2);

        System.out.println("Resultado" + result);


    return result;

    }

    @Path("windoh")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getItemsWindower() throws IOException, InterruptedException {

        List<String> newList= new ArrayList<String>();
        String topicName = "ShopPurchasesTopic";


        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(topicName);



        KTable<Windowed<String>, Long> countlinesWindow = lines.
                groupByKey().
                windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1))).count(Materialized.as("second"));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Press enter when ready...");
        System.in.read();

        ReadOnlyWindowStore<String, Long> store = streams.store("second",
                QueryableStoreTypes.windowStore());


        KeyValueIterator<Windowed<String>, Long> teste = store.all();

        while (teste.hasNext()) {
            KeyValue<Windowed<String>, Long> next = teste.next();
            System.out.println("count for " + next.key + " :Value " + next.value);
            String result = next.key.window().toString();
            newList.add(result);

        }





        return newList;

    }
    @Path("byKey")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getItemsByKey() throws IOException, InterruptedException {

        List<String> newList= new ArrayList<String>();
        String topicName = "ShopPurchasesTopic";


        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(topicName);

        KTable<String, Long> countlines = lines.groupByKey().count(Materialized.as("third"));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Press enter when ready...");
        System.in.read();


        ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store("third", QueryableStoreTypes.<String,Long>keyValueStore());

        String key = "Batatas";
        String key2 = "Cenouras";

        Long resultBatatas = keyValueStore.get(key);
        Long resultCenouras = keyValueStore.get(key2);

        System.out.println("Resultado batatas" +resultBatatas);
        System.out.println("Resultado cenouras" +resultCenouras);

        String result = "Batatas" + " " + Long.toString(resultBatatas)+ " " + "Cenouras" + " " + Long.toString(resultCenouras);


        newList.add(result);


        return newList;

    }

    @Path("highPrice")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getHighPriceItem() throws IOException, InterruptedException {

        List<String> newList= new ArrayList<String>();
        String topicName = "ShopPurchasesTopic";


        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(topicName);

        //lines.mapValues(v -> "" + v.split(" ");
        //lines.mapValues((k,v) -> "teste" + v.split(" ").length);

        //System.out.println(outlines);
        KTable<String, String> countlines = lines.map((k,v) -> KeyValue.pair(k,v.split(" ")[0])).groupByKey().reduce((oldVal,newVal)-> Integer.toString(Math.max(Integer.parseInt(newVal),Integer.parseInt(oldVal))),Materialized.as("forth"));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Press enter when ready...");
        System.in.read();


        ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("forth", QueryableStoreTypes.keyValueStore());

        //System.out.println(keyValueStore.get("Batatas"));

        String result = keyValueStore.get("Batatas");
        newList.add(result);


        return newList;

    }

    @Path("highPriceWindow")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getHighPriceItemWindow() throws IOException, InterruptedException {

        List<String> newList= new ArrayList<String>();
        String topicName = "ShopPurchasesTopic";


        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(topicName);

        //lines.mapValues(v -> "" + v.split(" ");
        //lines.mapValues((k,v) -> "teste" + v.split(" ").length);

        //System.out.println(outlines);
        KTable<Windowed<String>, String> countlines = lines.map((k,v) -> KeyValue.pair(k,v.split(" ")[0])).groupByKey().windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1))).reduce((oldVal,newVal)-> Integer.toString(Math.max(Integer.parseInt(newVal),Integer.parseInt(oldVal))),Materialized.as("fifth"));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Press enter when ready...");
        System.in.read();

        ReadOnlyWindowStore<String, Long> store = streams.store("fifth",
                QueryableStoreTypes.windowStore());
        KeyValueIterator<Windowed<String>, Long> teste = store.all();

        while (teste.hasNext()) {
            KeyValue<Windowed<String>, Long> next = teste.next();
            System.out.println("count for " + next.key.window().start() + " :Value " + next.value );
            String result = next.key.window().toString();
            newList.add(result);
        }




        return newList;

    }
    @Path("avgSupplies")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getAvgSupplier() throws IOException, InterruptedException {

        List<String> newList= new ArrayList<String>();
        String topicName = "ShopShipmentsTopic";


        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(topicName);

        //lines.mapValues(v -> "" + v.split(" ");
        //lines.mapValues((k,v) -> "teste" + v.split(" ").length);

        //System.out.println(outlines);
        KTable<String, Long> left = lines.map((k,v) -> KeyValue.pair(k.split(" ")[1],v)).groupByKey().count(Materialized.as("count"));
        KTable<String, String> right = lines.map((k,v) -> KeyValue.pair(k.split(" ")[1],v.split(" ")[1])).groupByKey().reduce((oldVal,newVal)-> Integer.toString((Integer.parseInt(newVal)+Integer.parseInt(oldVal))),Materialized.as("QuantidadeSum"));

        /* KTable<String, String> joinTable = left.leftJoin(right,
                new ValueJoiner<Long, String, String>() {
                    @Override
                    public String apply(Long leftValue, String rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },Materialized.as("qwe"));*/


        /* KTable<String, String> joinTable = left.leftJoin(right,(leftValue,rightValue)-> (rightValue/leftValye),Materialized.as("qwe"));
                },Materialized.as("qwe"));*/


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Press enter when ready...");
        System.in.read();


        ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("QuantidadeSum", QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<String, Long> countKeyValueStore = streams.store("count", QueryableStoreTypes.keyValueStore());
        //ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store("qwe", QueryableStoreTypes.keyValueStore());

        System.out.println("Soma da quantidade:"+keyValueStore.get("Batatas"));
        System.out.println("count:"+countKeyValueStore.get("Batatas"));
        System.out.println("AVG:"+Long.parseLong(keyValueStore.get("Batatas"))/countKeyValueStore.get("Batatas"));

        String result = Long.toString(Long.parseLong(keyValueStore.get("Batatas"))/countKeyValueStore.get("Batatas"));
        newList.add(result);


        return newList;

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void getRevenue() throws IOException, InterruptedException {

       // List<String> newList= new ArrayList<String>();

        System.out.println("TESTING");
        final String input = "123456";
        // my regexp:strong text
        // final String regex = "(!style_delete\\s\\[[a-zA-Z0-9\\s:]*\\])";
        // regexp from Trinmon:
        final String regex = "MyReplyTopic";

        final Matcher m = Pattern.compile(regex).matcher(input);

        final List<String> matches = new ArrayList<>();
        if(m.find()){
            System.out.println("IN");
        }else System.out.println("OUT");
        while (m.find()) {
            matches.add(m.group(0));
        }
        matches.get(0);
/*
        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        //<String, String> lines = builder.stream(topicName);*/




    }
}