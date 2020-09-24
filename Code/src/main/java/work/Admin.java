package work;

import com.mysql.cj.exceptions.StreamingNotifiable;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;


public class Admin {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target("http://localhost:9998/students");
        WebTarget webTarget1 = client.target("http://localhost:9998/students/xpto");
        WebTarget webTarget2 = client.target("http://localhost:9998/students/windoh");
        WebTarget webTarget3 = client.target("http://localhost:9998/students/byKey");
        WebTarget webTarget4 = client.target("http://localhost:9998/students/highPrice");
        WebTarget webTarget5 = client.target("http://localhost:9998/students/highPriceWindow");
        WebTarget webTarget6 = client.target("http://localhost:9998/students/avgSupplies");


        //webTarget.request().post(Entity.entity("Jose", MediaType.TEXT_PLAIN));



        System.out.println("Funcionalidades do Admin");
        System.out.println("1 - Número de items vendidos");
        System.out.println("2 - Pesquisar por item");
        System.out.println("3 - Pesquisar por item dentro de uma janela de tempo");
        System.out.println("4 - Pesquisar por item vendido por maior preço");
        System.out.println("5 - Item vendido por maior preço numa janela de tempo");
        System.out.println("6 - Numero médio de supplies");
        System.out.println("7 - Next Req");

        int menuOpt = sc.nextInt();

        if(menuOpt == 1){
            Invocation.Builder invocationBuilder =  webTarget1.request(MediaType.APPLICATION_JSON);
            System.out.println("Foram vendidos no total "+invocationBuilder.get(Long.class)+" items");
        }else if(menuOpt == 2){

            Invocation.Builder invocationBuilder =  webTarget3.request(MediaType.APPLICATION_JSON);
            List<String> response = invocationBuilder.get(List.class);
            System.out.println("Items By Key:" );
            for(String string : response){
                System.out.println(string);
            }
        }
        else if(menuOpt == 3){

            Invocation.Builder invocationBuilder =  webTarget2.request(MediaType.APPLICATION_JSON);
            List<String> response = invocationBuilder.get(List.class);
            System.out.println("Items By Key in a range of time:" );
            for(String string : response){
                System.out.println(string);
            }
        }
        else if(menuOpt == 4){

            Invocation.Builder invocationBuilder =  webTarget4.request(MediaType.APPLICATION_JSON);
            List<String> response = invocationBuilder.get(List.class);
            System.out.println("High Price:" );
            for(String string : response){
                System.out.println(string);
            }
        }
        else if(menuOpt == 5){

            Invocation.Builder invocationBuilder =  webTarget5.request(MediaType.APPLICATION_JSON);
            List<String> response = invocationBuilder.get(List.class);
            System.out.println("Items By Key in a range of time:" );
            for(String string : response){
                System.out.println(string);
            }
        }
        else if(menuOpt == 6){

            Invocation.Builder invocationBuilder =  webTarget6.request(MediaType.APPLICATION_JSON);
            List<String> response = invocationBuilder.get(List.class);
            System.out.println("AVG" );
            for(String string : response){
                System.out.println(string);
            }
        }
        else if(menuOpt == 7){

            Invocation.Builder invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
            invocationBuilder.get(List.class);
            /*System.out.println("NEXT REQ" );
            for(String string : response){
                System.out.println(string);
            }*/
        }
        else System.out.println("Funcionalidade não disponível");
    }

    //retorna o total de items vendidos
    static int getTotalSold(List<String> response){
        int count = 0;
        List<Item> newList = new ArrayList<>();
        String []parts;
        Double price;
        int quantidade;

        for(String string : response){

            parts = string.split(" ");

            for(int i  = 0; i <parts.length; i+=3) {
                Item newItem = new Item();
                count ++;
                price = Double.parseDouble(parts[i]);
                quantidade = Integer.parseInt(parts[i+1]);


                // System.out.println("price"+price+"--"+"Quantidade"+quantidade);
                newItem.setName("Batatas");
                newItem.setPrice(price);
                newItem.setQuantidade(quantidade);

                newList.add(newItem);
            }
        }

        for(Item item : newList){
            System.out.println(item);
        }

        return count;
    }
}