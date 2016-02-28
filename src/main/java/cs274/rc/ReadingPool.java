package cs274.rc;

import java.util.ArrayList;
import java.util.List;

public class ReadingPool {

	private String transaction;
	private String key;
	private List<ReadingData> list; // value and version

	public ReadingPool(String transaction, String key) {
		this.setTransaction(transaction);
		this.setKey(key);
		list = new ArrayList<ReadingData>();
	}

	public void addData(String replica, String value, long version) {
		for (ReadingData data : list) {
			if (data.replica.equals(replica)) {
				return;
			}
		}
		list.add(new ReadingData(replica, value, version));
	}

	public int getSize() {
		return list.size();
	}

	public String getTransaction() {
		return transaction;
	}

	public void setTransaction(String transaction) {
		this.transaction = transaction;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	private class ReadingData {
		public String replica;
		public String value;
		public long version;

		public ReadingData(String replica, String value, long version) {
			this.replica = replica;
			this.value = value;
			this.version = version;
		}
	}

}
