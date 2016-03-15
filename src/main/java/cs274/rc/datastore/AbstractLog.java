package cs274.rc.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class AbstractLog {

	private AbstractDatastore datastore;
	private List<LogEntry> log;

	public AbstractLog(AbstractDatastore datastore) {
		this.datastore = datastore;
		log = new ArrayList<LogEntry>();
	}

	public void put(LogEntry logEntry) {
		log.add(logEntry);
	}

	public void commit(long voteID, String transaction) {
		ListIterator<LogEntry> it = log.listIterator(log.size());
		while (it.hasPrevious()) {
			LogEntry entry = it.previous();
			if (entry.getTransaction().equals(transaction)) {
				for (String[] pair : entry.getWrites()) {
					datastore.put(pair[0], new DatastoreEntry(pair[1], voteID));
				}
			}
			it.remove();
		}

	}
}
