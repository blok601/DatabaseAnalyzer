package me.dhillon;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.Arrays;

public class Main {

    static double totalPoints = 0;
    static int totalAmount = 0;

    public static void main(String[] args){
        run();
    }

    private static void run(){
        System.out.println("Beginning to Connect to the Database");
        System.out.println("------------------------------------");
        final String URI = "mongodb://localhost:27017/network";
        MongoClient mongoClient = new MongoClient(new MongoClientURI(URI));

        MongoDatabase mongoDatabase = mongoClient.getDatabase("network");
        System.out.println("Connected to database: " + mongoDatabase.getName());
        System.out.println("------------------------------------");
        MongoCollection collection = mongoDatabase.getCollection("uhc_uhcplayers");
        System.out.println("Found Collection: uhc_uhcplayers");
        System.out.println("------------------------------------");
        System.out.println("Attempting to find average points...");
        System.out.println("This may take a while....");
        System.out.println("------------------------------------");
        //Average = (amount) + (amount)/# of elements
        System.out.println("Looping through database:");
        Block<Document> documentBlock = document -> {
            double p = (double) document.get("points");
            totalPoints+= p;
            totalAmount++;
        };

        collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.gte("points", 1))
                )
        ).forEach(documentBlock);

        System.out.println("Successfully Looped Through Database");
        System.out.println("------------------------------------");
        System.out.println("Calculating Final Results...");
        System.out.println("------------------------------------");
        System.out.println("Final Results:");
        System.out.println("Total Documents Found: " + totalAmount);
        System.out.println("Total Points: " + totalPoints);
        System.out.println("Average Points: " + totalPoints/totalAmount);
        System.out.println("------------------------------------");

    }


}
