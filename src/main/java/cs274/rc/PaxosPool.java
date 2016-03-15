package cs274.rc;

public class PaxosPool {

	private String transaction;
	private long voteID;
	private int acceptCount;
	private int rejectCount;

	public PaxosPool(String transaction, long voteID) {
		this.transaction = transaction;
		this.voteID = voteID;
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

	public long getVoteID() {
		return voteID;
	}

}
