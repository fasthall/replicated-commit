package cs274.rc;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
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

	public Client(String name, String hostname, int port,
			ClusterManager clusterManager) {
		this.name = name;
		this.hostname = hostname;
		this.port = port;
		this.clusterManager = clusterManager;
		readingPool = null;
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
				if (cmd[0].equals(Server.REPLY_READ)) {
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
		Operation operation = transaction.popOperation();
		while (operation != null) {
			if (operation.getAction() == Operation.READ) {
				readingPool = new ReadingPool(transaction.getName(),
						operation.getKey());
				sendReadRequest(transaction.getName(), operation.getKey(),
						operation.toString() + " " + transaction.getName()
								+ " " + hostname + " " + port);
				while (readingPool.getSize() < clusterManager
						.getMajorityNumber()) {
					// Waiting data from majority
				}
				String value = readingPool.getMostRecentValue();
				System.out.println("Most recent data of " + operation.getKey()
						+ " is " + value);
				readingPool = null;
			}
			operation = transaction.popOperation();
		}
	}

	public void sendReadRequest(String transaction, String key, String data)
			throws UnknownHostException, IOException {
		// Send read request to all replicas
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
