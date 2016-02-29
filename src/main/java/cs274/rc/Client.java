package cs274.rc;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import cs274.rc.connection.ClusterManager;
import cs274.rc.connection.ReplicaConnection;

public class Client extends Thread {

	private String name;
	private String hostname;
	private int port;
	private ServerSocket serverSocket;
	private ClusterManager clusterManager;

	private ReadingPool readingPool;
	private List<Operation> writeBuffer;

	public Client(String name, String hostname, int port,
			ClusterManager clusterManager) {
		this.name = name;
		this.hostname = hostname;
		this.port = port;
		this.clusterManager = clusterManager;
		readingPool = null;
		writeBuffer = null;
	}

	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(port);
			System.out.println("Server " + name + " starts listening on port "
					+ port);
			while (true) {
				Socket connectionSocket = serverSocket.accept();
				BufferedReader bufferedReader = new BufferedReader(
						new InputStreamReader(connectionSocket.getInputStream()));
				String received = bufferedReader.readLine();
				String[] cmd = received.split(" ");
				if (cmd[0].equals(Communication.REPLY_READ)) {
					if (readingPool != null
							&& cmd[2].equals(readingPool.getTransaction())
							&& cmd[3].equals(readingPool.getKey())) {
						readingPool.addDataFromReplica(cmd[1], cmd[4],
								Long.parseLong(cmd[5]));
					}
				}
				System.out.println("Client " + name + " received: " + received);
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

	public void put(Transaction transaction) throws UnknownHostException,
			IOException {
		while (true) {
			Operation operation = transaction.popOperation();
			if (operation == null) {
				// transaction terminates, start Paxos
				System.out.println("transaction terminates, start Paxos");
				// TODO appoint the coordinators at each DC
				sendPaxosRequest();
				break;
			} else if (operation.getAction() == Operation.READ) {
				sendReadRequest(transaction, operation);
			} else if (operation.getAction() == Operation.WRITE) {
				// buffer write
				writeBuffer.add(operation);
			}
		}
	}

	public synchronized void sendReadRequest(Transaction transaction,
			Operation operation) throws UnknownHostException, IOException {
		readingPool = new ReadingPool(transaction.getName(), operation.getKey());
		String data = operation.toString() + " " + transaction.getName() + " "
				+ hostname + " " + port;
		// Send read request to all replicas
		for (ReplicaConnection replica : clusterManager.getReplicas()) {
			send(data, replica.getHostname(), replica.getPort());
		}
		while (readingPool.getSize() <= clusterManager.getReplicaNumber() / 2) {
			// Waiting data from majority
		}
		String value = readingPool.getMostRecentValue();
		System.out.println("Most recent data of " + operation.getKey() + " is "
				+ value);
		readingPool = null;
	}

	public synchronized void sendPaxosRequest() throws UnknownHostException,
			IOException {
		// cmd[0] = PaxosRequest
		// cmd[1] = vote ID
		// cmd[2] = hostname
		// cmd[3] = port
		String data = Communication.PAXOS_REQUEST + " "
				+ System.currentTimeMillis() + " " + hostname + " " + port;
		// Send Paxos accept request to all the coordinators
		for (ReplicaConnection replica : clusterManager.getReplicas()) {
			send(data, replica.getHostname(), replica.getPort());
		}
	}

	public void send(String data, String hostname, int port)
			throws UnknownHostException, IOException {
		Socket clientSocket = new Socket(hostname, port);
		DataOutputStream outToServer = new DataOutputStream(
				clientSocket.getOutputStream());
		outToServer.writeBytes(data + '\n');
		clientSocket.close();
	}

}
