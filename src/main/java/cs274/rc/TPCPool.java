package cs274.rc;

public class TPCPool {

	private String transaction;
	private long voteID;
	private int acceptCount;
	private int rejectCount;

	public TPCPool(String transaction, long voteID) {
		this.transaction = transaction;
		this.voteID = voteID;
		acceptCount = 0;
		rejectCount = 0;
	}

	public void addAccept() {
		++acceptCount;
	}

	public void addReject() {
		++rejectCount;
	}

	public int getAcceptCount() {
		return acceptCount;
	}

	public int getRejectCount() {
		return rejectCount;
	}

	public String getTransction() {
		return transaction;
	}

	public long getVoteID() {
		return voteID;
	}

}
