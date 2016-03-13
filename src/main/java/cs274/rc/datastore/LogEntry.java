package cs274.rc.datastore;

import java.util.ArrayList;
import java.util.List;

public class LogEntry {

	public static final int TPC_PREPARE = 0;
	public static final int TPC_COMMIT = 1;

	public String transaction;
	public int type;
	public List<String[]> write;

	public LogEntry(String transaction, int type) {
		this.transaction = transaction;
		this.type = type;
		write = new ArrayList<String[]>();
	}

	public void addWrite(String key, String value) {
		write.add(new String[] { key, value });
	}
	
	public List<String[]> getWrite() {
		return write;
	}
	
}
