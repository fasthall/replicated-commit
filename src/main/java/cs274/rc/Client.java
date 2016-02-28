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

public class Client extends Thread {

	private String name;
	private String hostname;
	private int port;
	private ServerSocket serverSocket;
	private List<String> replicas;

	private ReadingPool readingPool;

	public Client(String name, String hostname, int port) {
		this.name = name;
		this.hostname = hostname;
		this.port = port;
		replicas = new ArrayList<String>();
		readingPool = null;
	}

	public void addReplica(String hostname, int port) {
		replicas.add(hostname + ":" + port);
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
						readingPool.addData(cmd[1], cmd[4],
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
				sendReadRequest(transaction.getName(), operation.getKey(),
						operation.toString() + " " + transaction.getName()
								+ " " + hostname + " " + port);
			}
			operation = transaction.popOperation();
		}
	}

	public void sendReadRequest(String transaction, String key, String data)
			throws UnknownHostException, IOException {
		readingPool = new ReadingPool(transaction, key);
		// Send read request to all replicas
		for (String replica : replicas) {
			String[] hostnameAndPort = replica.split(":");
			String hostname = hostnameAndPort[0];
			int port = Integer.parseInt(hostnameAndPort[1]);
			send(data, hostname, port);
		}
		while (readingPool.getSize() < (replicas.size() + 1) / 2) {
			// Waiting data from majority
		}
		readingPool = null;
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
