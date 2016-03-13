package cs274.rc.datastore;

import java.util.HashMap;
import java.util.Map;

public class AbstractDatastore {

	private Map<String, DatastoreEntry> map;

	public AbstractDatastore() {
		map = new HashMap<String, DatastoreEntry>();
	}

	public DatastoreEntry get(String key) {
		return map.get(key);
	}

	public void put(String key, DatastoreEntry entry) {
		map.put(key, entry);
	}

}
