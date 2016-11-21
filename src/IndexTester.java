/**
 * Created by nilson on 19/11/16.
 */
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.math.BigInteger;
import java.util.*;

public class IndexTester {
    public static void main(String[] args) {
//        int opAmount = Integer.getInteger(args[0]);
//        new IndexTester().benchmark(opAmount);
        new IndexTester().benchmark(100);
    }

    public Document generateRandomTuple() {
        Document document = new Document();
        Random random = new Random();
        String string = new BigInteger(10000, random).toString(32);
        for(int j = 0; j < 10; j++)
        {
//            string = new BigInteger(10000, random).toString(32);
            document.put("att" + (j+1), string);
        }
        return document;
    }
/*
    public List<Document> generateManyRandomTuples(MongoCollection<Document> collection, int dataAmount){
        List<Document> cache = new ArrayList();
        for(int i = 0; i < dataAmount; i++) {
            cache.add(generateRandomTuple());
        }
        return cache;
    }
*/
//    public String randString() {
//        String str = "";
//        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
//
//        Random rand = new Random();
//        for(int i=0; i < 2000; i++) {
//            str += chars.charAt(rand.nextInt(chars.length()));
//        }
//
//        return str;
//    }

    public void benchmark(int opAmount) {
        try {
            MongoClientURI uri = new MongoClientURI("mongodb://localhost:27017");
            MongoClient client = new MongoClient(uri);
            MongoDatabase db = client.getDatabase("index_test");
            MongoCollection<Document> collection = db.getCollection("test");

            Cache cache = new Cache(100);
            Document document;

            Long timeBefore = System.currentTimeMillis();
            for (int i = 0; i < opAmount; i++) {
                document = generateRandomTuple();
                cache.add(document);
                collection.insertOne(document);
            }
            Long timeAfter = System.currentTimeMillis();
            System.out.println((timeAfter - timeBefore)*1.0/1000);

            Document filter = new Document();
            timeBefore = System.currentTimeMillis();
            for (int i = 0; i < opAmount; i++) {
                document = cache.getRandom();
                filter.append("att1", document.get("att1"));
                collection.find();
                filter.clear();
            }
            timeAfter = System.currentTimeMillis();
            System.out.println((timeAfter - timeBefore)*1.0/1000);
        } catch (Exception e) { e.printStackTrace(); }
    }
}

