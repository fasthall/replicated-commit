package cs274.rc.protocol;

public class TPCPool {

	private int acceptCount;
	private int rejectCount;

	public TPCPool() {
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

}
