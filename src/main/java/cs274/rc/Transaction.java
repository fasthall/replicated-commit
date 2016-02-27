package cs274.rc;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

	private List<Operation> list;

	public Transaction() {
		list = new ArrayList<Operation>();
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

}
