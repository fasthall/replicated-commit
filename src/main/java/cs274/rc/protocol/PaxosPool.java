package cs274.rc.protocol;

public class PaxosPool {

	private String transaction;
	private int acceptCount;
	private int rejectCount;

	public PaxosPool(String transaction) {
		this.transaction = transaction;
		acceptCount = 0;
		rejectCount = 0;
	}

	public synchronized void addAccept() {
		++acceptCount;
	}

	public synchronized void addReject() {
		++rejectCount;
	}

	public synchronized int getAcceptCount() {
		return acceptCount;
	}

	public synchronized int getRejectCount() {
		return rejectCount;
	}

	public String getTransction() {
		return transaction;
	}

}
