package cs274.rc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client {

	public Client() {

	}

	public void put(Transaction transaction) {

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
