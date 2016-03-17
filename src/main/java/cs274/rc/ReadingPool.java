package cs274.rc;

import java.util.ArrayList;
import java.util.List;

public class ReadingPool {

	private String transaction;
	private String key;
	private List<ReadingData> list; // value and version
	private int rejected;

	public ReadingPool(String transaction, String key) {
		this.transaction = transaction;
		this.key = key;
		list = new ArrayList<ReadingData>();
		rejected = 0;
	}

	public void addDataFromReplica(String value,
			long version) {
		list.add(new ReadingData(value, version));
	}

	public void addReject() {
		++rejected;
	}

	public String getMostRecentValue() {
		String value = null;
		long version = 0;
		for (ReadingData data : list) {
			if (data.version > version) {
				version = data.version;
				value = data.value;
			}
		}
		return value;
	}

	public int getSize() {
		return list.size();
	}

	public int getReject() {
		return rejected;
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
		public String value;
		public long version;

		public ReadingData(String value, long version) {
			this.value = value;
			this.version = version;
		}
	}

}
