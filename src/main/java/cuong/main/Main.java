package cuong.main;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class Main {

    public static final String CONNECTION_STRING = "mongodb://localhost:27017";
    public static final String DATABASE_NAME = "school";
    public static final String COLLECTION_NAME = "students";
    public static final String FILE_NAME = "example.json";

    public static void main(String[] args) {

        try (MongoClient client = MongoClients.create(CONNECTION_STRING)) {
            MongoCollection<Document> collection = client
                    .getDatabase(DATABASE_NAME).
                    getCollection(COLLECTION_NAME);

            // 1. Đọc file đó và lưu vào mongodb
//            importFromJsonFileToDb(collection, FILE_NAME);
//            collection.find().forEach(d -> System.out.println(d.toJson()));

            // 2. Tìm kiếm dữ liệu filter theo name/type
            // 3. Load 1 item theo _id
//            Bson filter = Filters.eq("name", "Long");
//            filter = Filters.eq("degrees.type", "Cử nhân");
//            filter = Filters.eq("_id", new ObjectId("650876a45b30f667981e4f6f"));
//            findName(collection, filter);

            // 4. Thống kê số lượng người theo loại băng cấp (type), quốc gia (country)
            aggregationDemo2(collection, "country");
//            aggregationDemo2(collection, "type");

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * Thống kê số lượng người theo loại bằng cấp (type) hoặc quốc gia (country)
     * @param collection the name of collection
     * @param name the field name of degree (type, country)
     */
    public static void aggregationDemo2(MongoCollection<Document> collection, String name) {
        String groupName = "$degrees." + name;

        collection.aggregate(List.of(
                Aggregates.unwind("$degrees"),
                Aggregates.group(
                    groupName,
                    Accumulators.sum("total", 1)),
                Aggregates.sort(new Document("total", -1))
                )
        ).forEach(doc -> System.out.println(doc.toJson()));
    }
    public static void aggregationDemo(MongoCollection<Document> collection, String name) {
            String groupName = "$degrees." + name;

            collection.aggregate(List.of(
                    Aggregates.group(
                        groupName,
                        Accumulators.sum("totalStudent", 1))
                    )
            ).forEach(doc -> System.out.println(doc.toJson()));
    }

    /**
     * Print out the collections based on the query filter
     * @param collection the name of collection
     * @param filter the query filter
     */
    public static void findName(MongoCollection<Document> collection, Bson filter) {
            FindIterable<Document> documents = collection.find(filter);

            int count = 0;
            for (Document d : documents) {
                count++;
                System.out.println(d.toJson());
            }
            if (count == 0) {
                System.out.println("No document.");
            } else {
                System.out.println("Found " + count + " document(s).");
            }
    }

    /**
     * Import data from json file to database
     * @param collection the name of the collection to import
     */
    public static void importFromJsonFileToDb(MongoCollection<Document> collection, String fileName) {
        List<Student> students = convertJsonFileToList(fileName);
        if (students == null) return;

        List<Document> documents = convertStudentListToDocumentList(students);
        if (documents == null) return;

        insertMany(collection, documents);
    }
    /**
     * Insert a list of document objects to the database
     * @param collection the name of the collection to return
     * @param documents list of document objects
     */
    public static void insertMany(MongoCollection<Document> collection, List<Document> documents) {
            collection.drop();
            collection.insertMany(documents);
            System.out.println("Insert successfully " + documents.size() + " documents.");
    }
    /**
     * Convert student list to document list
     * @param students list of student objects
     * @return list of document objects, null if JsonProcessingException occurs
     */
    public static List<Document> convertStudentListToDocumentList(List<Student> students) {
        List<Document> documents = new ArrayList<>();
        for (Student student : students) {
            try {
                documents.add(Document.parse(new ObjectMapper().writeValueAsString(student)));
            } catch (JsonProcessingException e) {
                System.out.println("Error when converting list of Students to list of documents "
                        + e.getMessage());
                return null;
            }
        }
        return documents;
    }

    /**
     * Convert the content in json file to a list of student objects,
     * assuming the content is an array.
     * @param filename file that contains json array.
     * @return list of students, null if IOException occurs.
     */
    public static List<Student> convertJsonFileToList(String filename) {
        File file = new File(filename);
        ObjectMapper mapper = new ObjectMapper();
        try {
            return List.of(mapper.readValue(file, Student[].class));
        } catch (IOException e) {
            System.err.println("Error when parsing content of json file: " + e.getMessage());
            return null;
        }
    }
}
