package cs274.rc.datastore;

import com.mongodb.client.MongoDatabase;
import java.util.HashMap;
import java.util.Map;

public class AbstractDatastore {

	private Map<String, DatastoreEntry> map;
	private MongoDatabase db;

	public AbstractDatastore() {
		map = new HashMap<String, DatastoreEntry>();
		// MongoClient mongoClient = new MongoClient("localhost", 27017);
		// db = mongoClient.getDatabase("test");
	}

	public DatastoreEntry get(String key) {
		return map.get(key);
		// Document doc = db.getCollection("collection").find(new
		// Document("key", key)).first();
		// DatastoreEntry entry = new DatastoreEntry(doc.getString("value"),
		// doc.getLong("version"));
		// return entry;
	}

	public void put(String key, DatastoreEntry entry) {
		map.put(key, entry);
		// db.getCollection("collection").insertOne(new Document().append("key",
		// key).append("value", entry.getValue()).append("version",
		// entry.getVersion()));
	}

	public MongoDatabase getDb() {
		return db;
	}

	public void setDb(MongoDatabase db) {
		this.db = db;
	}

}
