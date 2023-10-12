package cuong.main.binary;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Binary;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Test {

    public static void storeBinaryFile() throws Exception {
        // Connect to MongoDB
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

        // Access the database
        MongoDatabase database = mongoClient.getDatabase("binaryFiles");

        // Access the collection
        MongoCollection<Document> collection = database.getCollection("myCollection");

        // Read the binary file
        byte[] fileContent = Files.readAllBytes(Paths.get("files/coffee.jpg"));

        // Create a new document with the binary data
        Document document = new Document("binaryData", fileContent);

        // Insert the document into the collection
        collection.insertOne(document);

        System.out.println("Binary file stored successfully in MongoDB.");

        // Close the connection
        mongoClient.close();
    }
    public static void retrieveBinaryFile() throws Exception {
        // Connect to MongoDB
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

        // Access the database
        MongoDatabase database = mongoClient.getDatabase("binaryFiles");

        // Access the collection
        MongoCollection<Document> collection = database.getCollection("myCollection");

        // Retrieve the document with the binary data
        Document document = collection.find().first();

        if (document == null) return;

        // Get the binary data from the document
        Binary binaryData = document.get("binaryData", Binary.class);
        byte[] fileContent = binaryData.getData();

        // Write the binary data to a file
        Files.write(Paths.get("files/output/coffee.jpg"), fileContent);

        System.out.println("Binary file retrieved successfully from MongoDB.");

        // Close the connection
        mongoClient.close();
    }
}
