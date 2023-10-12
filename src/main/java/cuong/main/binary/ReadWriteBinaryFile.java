package cuong.main.binary;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class ReadWriteBinaryFile {

    public static final String CONNECTION_STRING = "mongodb://localhost:27017";
    public static final String DATABASE_NAME = "binaryFiles";
    public static final String OUTPUT_FOLDER = "files/output";
    public static final String INPUT_FOLDER = "files/input";

    public static void main(String[] args) {
        String smallFile = "coffee.jpg";
        String largeFile = "largeFile.pdf";


        try (MongoClient mongoClient = MongoClients.create(CONNECTION_STRING)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            GridFSBucket gridFSBucket = GridFSBuckets.create(database);

//            storeLargeBinaryFile(gridFSBucket, smallFile);
//            storeLargeBinaryFile(gridFSBucket, largeFile);

            retrieveLargeBinaryFile(gridFSBucket, smallFile);
            retrieveLargeBinaryFile(gridFSBucket, largeFile);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }

    public static void retrieveLargeBinaryFile(GridFSBucket gridFSBucket, String filename) {
        if (!isFileExistInDb(filename)) {
            System.out.println("File: " + filename + " doesn't existed on database!");
            return;
        }

        Path filePath = Path.of(OUTPUT_FOLDER, filename);
        if (Files.exists(filePath)) {
            try {
                Files.deleteIfExists(filePath);
                System.out.println("Delete file: " + filename + " successfully!");
                Files.createFile(filePath);
            } catch (IOException e) {
                System.out.println(e.getMessage());
                return;
            }
        }

        readFileFromDb(gridFSBucket, filePath);

    }
    public static void readFileFromDb(GridFSBucket gridFSBucket, Path filePath) {
        String filename = String.valueOf(filePath.getFileName());

        // Get the output stream of the file
        try (BufferedOutputStream streamToDownloadTo = new BufferedOutputStream(
                new FileOutputStream(filePath.toFile()))) {

            // Download the file from GridFS
            gridFSBucket.downloadToStream(filename, streamToDownloadTo);
            System.out.println("File " + filename + " retrieved successfully from MongoDB.");
        } catch (IOException e) {
            System.err.println("Error when reading file from database");
            System.err.println(e.getMessage());
        }
    }

    public static void storeLargeBinaryFile(GridFSBucket gridFSBucket, String filename) {
        Path filePath = Path.of(INPUT_FOLDER, filename);
        if (!Files.exists(filePath)) {
            System.out.println("File " + filename + " does not exist on disk!");
            return;
        }

        if (isFileExistInDb(filename)) {
            System.out.println("File: " + filename + " already existed on database!");
            return;
        }

        writeFileToDb(gridFSBucket, filePath);
    }

    public static void writeFileToDb(GridFSBucket gridFSBucket, Path filePath) {
        ObjectId fileId;
        String filename = String.valueOf(filePath.getFileName());

        // Get the input stream of the file
        try (BufferedInputStream streamToUploadFrom = new BufferedInputStream(
                new FileInputStream(filePath.toFile()))) {

            // Create some custom options
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .chunkSizeBytes(1024 * 1024) // 1MB
                    .metadata(new Document("type", "binary"));

            // Upload the file to GridFS
            fileId = gridFSBucket.uploadFromStream(filename, streamToUploadFrom, options);

            System.out.println("File: " + filename
                    + " stored successfully in MongoDB with file ID: " + fileId);

        } catch (IOException e) {
            System.err.println("Error when writing file to database" + filename);
            System.err.println("Error: " + e.getMessage());
        }
    }
    public static boolean isFileExistInDb(String filename) {
        try (MongoClient mongoClient = MongoClients.create(CONNECTION_STRING)) {
            var collection = mongoClient
                    .getDatabase(DATABASE_NAME)
                    .getCollection("fs.files");
            Document result = collection
                    .find(Filters.eq("filename", filename))
                    .first();
            return result != null;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
    }

}
