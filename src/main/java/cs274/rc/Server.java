package cs274.rc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Server extends Thread {

	private String name;
	private int port;
	private ServerSocket serverSocket;

	public Server(String name, int port) {
		this.name = name;
		this.port = port;
	}

	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(port);
			System.out.println(name + " starts listening on port " + port);
			while (true) {
				Socket connectionSocket = serverSocket.accept();
				BufferedReader inFromClient = new BufferedReader(
						new InputStreamReader(connectionSocket.getInputStream()));
				String clientSentence = inFromClient.readLine();
				System.out.println("Received: " + clientSentence);
				if (clientSentence.equals("exit")) {
					break;
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

}
