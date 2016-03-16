/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

public class Operation {

	public static final int READ = 0;
	public static final int WRITE = 1;

	private String transaction;
	private int action;
	private String key;
	private String value;

	public Operation(String transaction, int action, String key) {
		this.action = action;
		this.key = key;
	}

	public Operation(String transaction, int action, String key, String value) {
		this.transaction = transaction;
		this.action = action;
		this.key = key;
		this.value = value;
	}

	public String getTransaction() {
		return transaction;
	}

	public void setTransaction(String transaction) {
		this.transaction = transaction;
	}

	public int getAction() {
		return action;
	}

	public void setAction(int action) {
		this.action = action;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		if (action == READ) {
			// cmd[0] = "Read"
			// cmd[1] = key
			// cmd[2] = transaction
			return Communication.OPERATION_READ + " " + key + " " + transaction;
		} else if (action == WRITE) {
			// cmd[0] = "Write"
			// cmd[1] = key
			// cmd[2] = value
			// cmd[3] = transaction
			// cmd[4] = hostname
			// cmd[5] = port
			return Communication.OPERATION_WRITE + " " + key + " " + value
					+ " " + transaction;
		} else {
			return "Incorrect operation";
		}
	}

}
