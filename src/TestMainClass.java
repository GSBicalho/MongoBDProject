import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class TestMainClass {

	public static void main(String[] args) {
        // TODO code application logic here
        try {             /*Connect*/
            MongoClient mongo = new MongoClient("localhost", 27017);
            MongoDatabase db = mongo.getDatabase("labbd2016");
            MongoCollection table = db.getCollection("alunos");
            /*Insert*/
            Document document = new Document();
            document.put("nome", "Adao2");
            document.put("idade", 35);
            table.insertOne(document);
            for (Object doc : table.find()) {
            	Document a = (Document) doc;
                System.out.println(a.toJson());
            }            
            /*Find*/
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("idade", new BasicDBObject("$gt",1));
            MongoCursor<Document> cursor = table.find(searchQuery).iterator();
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
