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

import cs274.rc.connection.ClusterManager;
import cs274.rc.connection.Node;

public class Client extends Thread {

	private String name;
	private String hostname;
	private int port;
	private ServerSocket serverSocket;
	private ClusterManager clusterManager;

	private HashMap<Long, ReadingPool> readingPools;
	private HashMap<Long, PaxosPool> paxosPools;

	public Client(String name, String hostname, int port,
			ClusterManager clusterManager) {
		this.name = name;
		this.hostname = hostname;
		this.port = port;
		this.clusterManager = clusterManager;
		readingPools = new HashMap<Long, ReadingPool>();
		paxosPools = new HashMap<Long, PaxosPool>();
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
				System.out.println("Client " + name + " received: " + received);
				new Thread() {
					@Override
					public void run() {
						try {
							handleOperation(received);
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

	private void handleOperation(String received) {
		String[] cmd = received.split(" ");
		if (cmd[0].equals(Communication.READ_REPLY)) {
			long voteID = Long.parseLong(cmd[3]);
			if (readingPools.containsKey(voteID)
					&& readingPools.get(voteID).getTransaction().equals(cmd[2])
					&& readingPools.get(voteID).getKey().equals(cmd[4])) {
				readingPools.get(voteID).addDataFromReplica(cmd[1], cmd[5],
						Long.parseLong(cmd[6]));
			}
		} else if (cmd[0].equals(Communication.READ_REJECT)) {
			long voteID = Long.parseLong(cmd[3]);
			if (readingPools.containsKey(voteID)
					&& readingPools.get(voteID).getTransaction().equals(cmd[2])
					&& readingPools.get(voteID).getKey().equals(cmd[4])) {
				readingPools.get(voteID).addReject();
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
				;
			}
		}
	}

	public boolean put(Transaction transaction) throws UnknownHostException,
			IOException {
		boolean result = false;
		List<Operation> writeBuffer = new ArrayList<Operation>();
		while (true) {
			Operation operation = transaction.popOperation();
			if (operation == null) {
				// transaction terminates, start Paxos
				System.out.println("transaction terminates, start Paxos");
				result = sendPaxosRequest(transaction, writeBuffer);
				break;
			} else if (operation.getAction() == Operation.READ) {
				boolean readResult = sendReadRequest(transaction, operation);
				if (!readResult) {
					writeBuffer.clear();
					sendPaxosRequest(transaction, writeBuffer);
					return readResult;
				}
			} else if (operation.getAction() == Operation.WRITE) {
				// buffer write
				writeBuffer.add(operation);
			}
		}
		return result;
	}

	private synchronized boolean sendReadRequest(Transaction transaction,
			Operation operation) throws UnknownHostException, IOException {
		boolean result = true;
		long voteID = transaction.getVoteID();
		readingPools.put(voteID, new ReadingPool(transaction.getName(),
				operation.getKey()));
		String data = operation.toString() + " " + transaction.getName() + " "
				+ voteID + " " + hostname + " " + port;
		// Send read request to all replicas
		for (Node replica : clusterManager.getReplicas()) {
			send(data, replica.getHostname(), replica.getPort());
		}
		long startTime = System.currentTimeMillis();
		ReadingPool readingPool = readingPools.get(voteID);
		while (readingPool.getSize() <= clusterManager.getReplicaNumber() / 2
				&& readingPool.getSize() + readingPool.getReject() < clusterManager
						.getReplicaNumber()) {
			// Waiting data from majority
		}
		if (readingPool.getSize() <= clusterManager.getReplicaNumber() / 2) {
			result = false;
			System.out.println("Read " + operation.getKey() + " aborts.");
		} else {
			String value = readingPool.getMostRecentValue();
			System.out.println("Most recent data of " + operation.getKey()
					+ " is " + value);
		}
		readingPools.remove(voteID);
		return result;
	}

	private synchronized boolean sendPaxosRequest(Transaction transaction,
			List<Operation> writeBuffer) throws UnknownHostException,
			IOException {
		// cmd[0] = PaxosRequest
		// cmd[1] = vote ID
		// cmd[2] = writeBuffer
		// cmd[3] = transaction
		// cmd[4] = hostname
		// cmd[5] = port
		long voteID = transaction.getVoteID();
		paxosPools.put(voteID, new PaxosPool(transaction.getName(), voteID));
		String data = Communication.PAXOS_REQUEST + " " + voteID + " "
				+ serializeBuffer(writeBuffer) + " " + transaction.getName()
				+ " " + hostname + " " + port;
		// Send Paxos accept request to all the coordinators
		for (Node replica : clusterManager.getReplicas()) {
			if (replica.isCoordinator()) {
				send(data, replica.getHostname(), replica.getPort());
			}
		}
		long startTime = System.currentTimeMillis();
		PaxosPool paxosPool = paxosPools.get(voteID);
		while (paxosPool.getAcceptCount() + paxosPool.getRejectCount() < clusterManager
				.getCoordinatorNumber()
				|| paxosPool.getAcceptCount() <= clusterManager
						.getCoordinatorNumber() / 2) {
			// Wait for majority
//			if (System.currentTimeMillis() > startTime + 500) {
//				// timeout
//				break;
//			}
		}
		boolean result;
		if (paxosPool.getAcceptCount() > clusterManager.getCoordinatorNumber() / 2) {
			// commit success from client's view
			System.out.println("Client " + name + " successfully commits "
					+ transaction.getName() + ".");
			result = true;
		} else {
			// abort
			System.out.println("Not enough accepts, abort.");
			result = false;
		}
		paxosPools.remove(voteID);
		return result;
	}

	private synchronized void send(String data, String hostname, int port)
			throws UnknownHostException, IOException {
		Socket clientSocket = new Socket(hostname, port);
		DataOutputStream outToServer = new DataOutputStream(
				clientSocket.getOutputStream());
		outToServer.writeBytes(data + '\n');
		clientSocket.close();
	}

	public String serializeBuffer(List<Operation> writeBuffer) {
		if (writeBuffer.isEmpty())
			return ",";
		String serializedBuffer = "";
		for (Operation operation : writeBuffer) {
			serializedBuffer += operation.getKey() + ":" + operation.getValue()
					+ ",";
		}
		return serializedBuffer;
	}
}
