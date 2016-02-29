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

public class Server extends Thread {

	private String name;
	private int port;
	private ServerSocket serverSocket;
	private LockManager lockManager;

	public Server(String name, int port) {
		this.name = name;
		this.port = port;
		lockManager = new LockManager();
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
				if (received.equals("exit")) {
					break;
				}
				System.out.println("Server " + name + " received: " + received);
				boolean operationResult = handlerOperation(received);
				if (operationResult == false) {
					// abort transaction
				}
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

	private boolean handlerOperation(String operation)
			throws NumberFormatException, UnknownHostException, IOException {
		String[] cmd = operation.split(" ");
		// cmd[0] = "Read" / "Write"
		// cmd[1] = key
		// cmd[2] = value (not used in read
		// cmd[3] = transaction
		// cmd[4] = hostname
		// cmd[5] = port
		if (cmd[0].equals("Read")) {
			return handleRead(cmd[1], cmd[3], cmd[4], cmd[5]);
		} else if (cmd[0].equals("Write")) {
			return handleWrite(cmd[1], cmd[2], cmd[3], cmd[4], cmd[5]);
		} else if (cmd[0].equals(Communication.PAXOS_REQUEST)) {
			/*
			 * TODO The coordinator sends 2PC prepare request to all cohorts
			 * within the same DC, including the coordinator itself All cohorts
			 * acquire locks and log the 2PC prepare operation The coordinator
			 * waits for acknowledgments from all cohorts within the same DC
			 * that they are prepared
			 */
			return false;
		} else {
			return false;
		}
	}

	private boolean handleRead(String key, String transaction, String hostname,
			String port) throws NumberFormatException, UnknownHostException,
			IOException {
		// set the shared lock
		if (lockManager.setShared(key, transaction)) {
			// successfully set, return the latest version

			// TODO get data from DB and send it back
			String value = "dataFromDB" + System.currentTimeMillis();
			long version = System.currentTimeMillis();
			String data = Communication.REPLY_READ + " " + name + " " + transaction + " "
					+ key + " " + value + " " + version;
			send(data, hostname, Integer.parseInt(port));
			return true;
		} else {
			// can't acquire the lock
			return false;
		}
	}

	private boolean handleWrite(String key, String value, String transaction,
			String hostname, String port) {
		return false;
	}

	private void send(String data, String hostname, int port)
			throws UnknownHostException, IOException {
		Socket clientSocket = new Socket(hostname, port);
		DataOutputStream outToServer = new DataOutputStream(
				clientSocket.getOutputStream());
		outToServer.writeBytes(data + '\n');
		clientSocket.close();
		System.out.println("Server " + name + " sent: " + data);
	}

}
