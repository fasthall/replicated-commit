/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

	private String name;
	private List<Operation> list;

	public Transaction(String name) {
		this.name = name;
		list = new ArrayList<Operation>();
	}

	public void addReadOperation(String key) {
		list.add(new Operation(name, Operation.READ, key));
	}

	public void addWriteOperation(String key, String value) {
		list.add(new Operation(name, Operation.WRITE, key, value));
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

	public void setName(String name) {
		this.name = name;
	}

}
