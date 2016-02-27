package cs274.rc;

public class Operation {

	public static final int READ = 0;
	public static final int WRITE = 1;

	private int action;
	private String key;
	private String value;

	public Operation(int action, String key) {
		this.action = action;
		this.key = key;
	}

	public Operation(int action, String key, String value) {
		this.action = action;
		this.key = key;
		this.value = value;
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
			return "READ " + key;
		} else if (action == WRITE) {
			return "WRITE " + key + " " + value;
		} else {
			return "Incorrect operation";
		}
	}

}
