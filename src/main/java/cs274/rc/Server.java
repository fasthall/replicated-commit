/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cs274.rc.connection.DataCenterManager;
import cs274.rc.connection.Node;
import cs274.rc.datastore.AbstractDatastore;
import cs274.rc.datastore.AbstractLog;
import cs274.rc.datastore.DatastoreEntry;
import cs274.rc.datastore.LogEntry;

public class Server extends Thread {

	private String name;
	private String hostname;
	private int port;
	private ServerSocket serverSocket;
	private LockManager lockManager;
	private DataCenterManager dcManager;
	private List<Node> otherCoordinators;
	private AbstractDatastore datastore;
	private AbstractLog abstractLog;
	private HashMap<Long, TPCPool> tpcPools;
	private HashMap<Long, PaxosPool> paxosPools;

	public Server(String name, String hostname, int port, boolean coordinator) {
		this.name = name;
		this.hostname = hostname;
		this.port = port;
		lockManager = new LockManager();
		dcManager = new DataCenterManager();
		otherCoordinators = new ArrayList<Node>();
		datastore = new AbstractDatastore();
		abstractLog = new AbstractLog(datastore);
		dcManager.addShard(new Node(hostname, port, coordinator));
		tpcPools = new HashMap<Long, TPCPool>();
		paxosPools = new HashMap<Long, PaxosPool>();
	}

	public void addOtherCoordinator(Node node) {
		otherCoordinators.add(node);
	}

	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(port);
			// System.out.println("Server " + name +
			// " starts listening on port "
			// + port);
			while (true) {
				Socket connectionSocket = serverSocket.accept();
				BufferedReader bufferedReader = new BufferedReader(
						new InputStreamReader(connectionSocket.getInputStream()));
				final String received = bufferedReader.readLine();
				if (received.equals("exit")) {
					break;
				}
				System.out.println("Server " + name + " received: " + received);
				new Thread() {
					@Override
					public void run() {
						try {
							handlerOperation(received);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void handlerOperation(String operation)
			throws NumberFormatException, UnknownHostException, IOException {
		String[] cmd = operation.split(" ");
		// cmd[0] = "Read" / "Write"
		// cmd[1] = key
		// cmd[2] = value (not used in read)
		// cmd[3] = transaction
		// cmd[4] = voteID
		// cmd[5] = hostname
		// cmd[6] = port
		if (cmd[0].equals(Communication.OPERATION_READ)) {
			handleRead(cmd[1], cmd[3], Long.parseLong(cmd[4]), cmd[5],
					Integer.parseInt(cmd[6]));
		} else if (cmd[0].equals(Communication.PAXOS_REQUEST)) {
			// cmd[0] = PaxosRequest
			// cmd[1] = vote ID
			// cmd[2] = writeBuffer
			// cmd[3] = transaction
			// cmd[4] = hostname
			// cmd[5] = port
			handlePaxosRequest(Long.parseLong(cmd[1]), cmd[2], cmd[3], cmd[4],
					Integer.parseInt(cmd[5]));
		} else if (cmd[0].equals(Communication.TPC_PREPARE)) {
			// cmd[0] = 2PCPrepare
			// cmd[1] = vote ID
			// cmd[2] = writeBuffer
			// cmd[3] = transaction
			// cmd[4] = hostname
			// cmd[5] = port
			handle2PCPrepare(Long.parseLong(cmd[1]), cmd[2].split(","), cmd[3],
					cmd[4], Integer.parseInt(cmd[5]));
		} else if (cmd[0].equals(Communication.TPC_ACCEPT)) {
			long voteID = Long.parseLong(cmd[1]);
			if (tpcPools.containsKey(voteID)
					&& tpcPools.get(voteID).getTransction().equals(cmd[3])) {
				tpcPools.get(voteID).addAccept();
			}
		} else if (cmd[0].equals(Communication.TPC_REJECT)) {
			long voteID = Long.parseLong(cmd[1]);
			if (tpcPools.containsKey(voteID)
					&& tpcPools.get(voteID).getTransction().equals(cmd[3])) {
				tpcPools.get(voteID).addReject();
			}
		} else if (cmd[0].equals(Communication.PAXOS_ACCEPT)) {
			long voteID = Long.parseLong(cmd[1]);
			if (paxosPools.containsKey(voteID)
					&& paxosPools.get(voteID).getTransction().equals(cmd[2])) {
				paxosPools.get(voteID).addAccept();
			}
		} else if (cmd[0].equals(Communication.PAXOS_REJECT)) {
			long voteID = Long.parseLong(cmd[1]);
			if (paxosPools.containsKey(voteID)
					&& paxosPools.get(voteID).getTransction().equals(cmd[2])) {
				paxosPools.get(voteID).addReject();
			}
		}
	}

	private void handleRead(String key, String transaction, long voteID,
			String hostname, int port) throws NumberFormatException,
			UnknownHostException, IOException {
		// cmd[0] = ReadReply/Reject
		// cmd[1] = name
		// cmd[2] = transaction
		// cmd[3] = voteID
		// cmd[4] = key
		// cmd[5] = value
		// cmd[6] = version
		// set the shared lock
		if (lockManager.setShared(key, transaction)) {
			// successfully set, return the latest version
			DatastoreEntry entry = datastore.get(key);
			String value = (entry == null ? "NULL" : entry.getValue());
			long version = (entry == null ? 0 : entry.getVersion());
			String data = Communication.READ_REPLY + " " + name + " "
					+ transaction + " " + voteID + " " + key + " " + value
					+ " " + version;
			send(data, hostname, port);
		} else {
			System.out.println("CANNOT SET " + key + " for " + transaction);
			// can't acquire the lock
			String data = Communication.READ_REJECT + " " + name + " "
					+ transaction + " " + voteID + " " + key;
			send(data, hostname, port);
		}
	}

	private void handlePaxosRequest(long voteID, String writeBuffer,
			String transaction, String senderHostname, int senderPort)
			throws UnknownHostException, IOException {
		// send 2PC prepare to cohorts and self
		tpcPools.put(voteID, new TPCPool(transaction, voteID));
		TPCPool tpcPool = tpcPools.get(voteID);
		// wait until all reply
		for (Node node : dcManager.getShards()) {
			// cmd[0] = 2PCPrepare
			// cmd[1] = vote ID
			// cmd[2] = writeBuffer
			// cmd[3] = transaction
			// cmd[4] = hostname
			// cmd[5] = port
			if (!node.isCoordinator()) {
				String data = Communication.TPC_PREPARE + " " + voteID + " "
						+ writeBuffer + " " + transaction + " " + hostname
						+ " " + port;
				send(data, node.getHostname(), node.getPort());
			}
			handle2PCPrepareSelf(voteID, writeBuffer.split(","), transaction);
		}
		long startTime = System.currentTimeMillis();
		while (tpcPool.getAcceptCount() + tpcPool.getRejectCount() < dcManager
				.getShardsNumber()) {
			// Wait for majority
//			if (System.currentTimeMillis() > startTime + 500) {
//				// timeout
//				break;
//			}
		}
		boolean tpcResult;
		String data;
		if (tpcPool.getRejectCount() == 0) {
			tpcResult = true;
			data = Communication.PAXOS_ACCEPT + " " + voteID + " "
					+ transaction;
		} else {
			tpcResult = false;
			data = Communication.PAXOS_REJECT + " " + voteID + " "
					+ transaction;
		}
		send(data, senderHostname, senderPort);
		tpcPools.remove(voteID);

		paxosPools.put(voteID, new PaxosPool(transaction, voteID));
		PaxosPool paxosPool = paxosPools.get(voteID);
		if (tpcResult) {
			paxosPool.addAccept();
		} else {
			paxosPool.addReject();
		}
		for (Node node : otherCoordinators) {
			send(data, node.getHostname(), node.getPort());
		}
		startTime = System.currentTimeMillis();
		while (paxosPool.getAcceptCount() + paxosPool.getRejectCount() < otherCoordinators
				.size() + 1
				&& paxosPool.getAcceptCount() <= (otherCoordinators.size() + 1) / 2) {
			// Wait for majority
//			if (System.currentTimeMillis() > startTime + 500) {
//				// timeout
//				++App.errtimeout;
//				App.err4Str = App.err4Str
//						+ (paxosPool.getAcceptCount() + "+"
//								+ paxosPool.getRejectCount() + "\n");
//				break;
//			}
		}
		System.out.println("Server " + name
				+ " received from majority, start 2PC commit.");
		paxosPools.remove(voteID);

		/*
		 * Send 2PC commit. cohorts who receive this should log commit and
		 * release locks
		 */
		for (Node node : dcManager.getShards()) {
			// cmd[0] = 2PCPrepare
			// cmd[1] = vote ID
			// cmd[2] = transaction
			if (!node.isCoordinator()) {
				send(Communication.TPC_COMMIT + " " + voteID + " "
						+ writeBuffer, node.getHostname(), node.getPort());
			}
			handle2PCCommit(voteID, transaction);
		}
	}

	private void handle2PCPrepare(long voteID, String[] writeBuffer,
			String transaction, String senderHostname, int senderPort)
			throws UnknownHostException, IOException {
		// acquire all the exclusive locks
		List<String> keys = new ArrayList<String>();
		for (String writeOp : writeBuffer) {
			keys.add(writeOp.split(":")[0]);
		}
		if (lockManager.testExclusive(keys)) {
			LogEntry logEntry = new LogEntry(transaction, LogEntry.TPC_PREPARE);
			for (String writeOp : writeBuffer) {
				String key = writeOp.split(":")[0];
				String value = writeOp.split(":")[1];
				logEntry.addWrite(key, value);
				lockManager.setExclusive(key, transaction);
			}
			lockManager.unlockAllExclusiveByTransaction(transaction);
			abstractLog.put(logEntry);
			System.out.println("Server " + name + " locks: log 2PC prepare.");
			String data = Communication.TPC_ACCEPT + " " + voteID + " "
					+ transaction;
			send(data, senderHostname, senderPort);
		} else {
			String data = Communication.TPC_REJECT + " " + voteID + " "
					+ transaction;
			lockManager.unlockAllExclusiveByTransaction(transaction);
			send(data, senderHostname, senderPort);
		}
	}

	private void handle2PCPrepareSelf(long voteID, String[] writeBuffer,
			String transaction) {
		// acquire all the exclusive locks
		List<String> keys = new ArrayList<String>();
		for (String writeOp : writeBuffer) {
			keys.add(writeOp.split(":")[0]);
		}
		if (lockManager.testExclusive(keys)) {
			LogEntry logEntry = new LogEntry(transaction, LogEntry.TPC_PREPARE);
			for (String writeOp : writeBuffer) {
				String key = writeOp.split(":")[0];
				String value = writeOp.split(":")[1];
				logEntry.addWrite(key, value);
				lockManager.setExclusive(key, transaction);
			}
			abstractLog.put(logEntry);
			System.out.println("Server " + name + " locks: log 2PC prepare.");
			if (tpcPools.containsKey(voteID)
					&& tpcPools.get(voteID).getTransction().equals(transaction)) {
				tpcPools.get(voteID).addAccept();
			}
		} else {
			if (tpcPools.containsKey(voteID)
					&& tpcPools.get(voteID).getTransction().equals(transaction)) {
				tpcPools.get(voteID).addReject();
			}
		}
	}

	private void handle2PCCommit(long voteID, String transaction) {
		abstractLog.commit(voteID, transaction);
		lockManager.unlockAllExclusiveByTransaction(transaction);
		System.out.println("Server " + name
				+ " committed locally and released the locks.");
	}

	private void send(String data, String hostname, int port)
			throws UnknownHostException, IOException {
		Socket clientSocket = new Socket(hostname, port);
		DataOutputStream outToServer = new DataOutputStream(
				clientSocket.getOutputStream());
		outToServer.writeBytes(data + '\n');
		clientSocket.close();
		System.out.println("Server " + name + " sent to " + port + ": " + data);
	}

}
