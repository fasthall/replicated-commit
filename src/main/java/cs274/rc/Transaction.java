/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Transaction {

	private String name;
	private List<Operation> list;
	private List<String> readSet;
	private List<String> writeSet;

	public Transaction() {
		this.name = UUID.randomUUID().toString();
		list = new ArrayList<Operation>();
		readSet = new ArrayList<String>();
		writeSet = new ArrayList<String>();
	}

	public Transaction(String name) {
		this.name = name;
		list = new ArrayList<Operation>();
		readSet = new ArrayList<String>();
		writeSet = new ArrayList<String>();
	}

	public void addReadOperation(String key) {
		list.add(new Operation(name, Operation.READ, key));
		readSet.add(key);
	}

	public void addWriteOperation(String key, String value) {
		list.add(new Operation(name, Operation.WRITE, key, value));
		writeSet.add(key);
	}

	public void addOperation(Operation operation) {
		list.add(operation);
	}

	public Operation popOperation() {
		if (list.isEmpty()) {
			return null;
		}
		Operation operation = list.get(0);
		list.remove(0);
		return operation;
	}

	public String getName() {
		return name;
	}

	public List<String> getReadSet() {
		return readSet;
	}

	public List<String> getWriteSet() {
		return writeSet;
	}

}
