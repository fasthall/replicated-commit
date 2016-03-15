package cs274.rc.datastore;

import java.util.ArrayList;
import java.util.List;

public class LogEntry {

	public static final int TPC_PREPARE = 0;
	public static final int TPC_COMMIT = 1;

	private String transaction;
	private int type;
	private List<String[]> writes;

	public LogEntry(String transaction, int type) {
		this.setTransaction(transaction);
		this.setType(type);
		writes = new ArrayList<String[]>();
	}

	public void addWrite(String key, String value) {
		writes.add(new String[] { key, value });
	}

	public List<String[]> getWrites() {
		return writes;
	}

	public String getTransaction() {
		return transaction;
	}

	public void setTransaction(String transaction) {
		this.transaction = transaction;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

}
