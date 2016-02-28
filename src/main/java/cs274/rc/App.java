/*
 * @Author Wei-Tsung Lin
 * @Date 02/26/2016
 */
package cs274.rc;

import java.io.IOException;
import java.net.UnknownHostException;

import cs274.rc.connection.ClusterManager;

public class App {

	public static void main(String[] args) {
		System.out.println("Hello World!");

		// Start data centers
		Server server = new Server("DC1", 30001);
		server.start();

		ClusterManager clusterManager = ClusterManager.getInstance();
		clusterManager.addReplica("localhost", 30001);

		Client client = new Client();
		try {
			client.send("TEST1", "localhost", 30001);
			client.send("TEST2", "localhost", 30001);
			client.send("TEST3", "localhost", 30001);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
