package cs274.rc.datastore;

import java.util.ArrayList;
import java.util.List;

public class AbstractLog {

	List<LogEntry> log;

	public AbstractLog() {
		log = new ArrayList<LogEntry>();
	}

	public void put(LogEntry logEntry) {
		log.add(logEntry);
	}

	public LogEntry commit(String transaction) {
		LogEntry entry = null;
		int i;
		for (i = 0; i < log.size(); ++i) {
			if (log.get(i).transaction == transaction) {
				entry = log.get(i);
				break;
			}
		}
		log.remove(i);
		return entry;
	}

}
