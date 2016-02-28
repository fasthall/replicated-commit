/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.IOException;
import java.net.UnknownHostException;

import cs274.rc.connection.ClusterManager;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CommunicationTest extends TestCase {

	private int serverPort1 = 30001;
	private int clientPort1 = 40001;

	public CommunicationTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(CommunicationTest.class);
	}

	public void testClientToServer() throws UnknownHostException, IOException,
			InterruptedException {
		// System.out.println("testClientToServer");
		// Server server = new Server("S1", serverPort1);
		// server.start();
		// assertTrue(server.isAlive());
		// Client client = new Client("C1", clientPort1);
		// client.send("TEST DATA", "localhost", serverPort1);
		// assertTrue(server.isAlive());
		// client.send("exit", "localhost", serverPort1);
		// Thread.sleep(50);
		// assertFalse(server.isAlive());
	}

	public void testReadRequest() throws UnknownHostException, IOException,
			InterruptedException {
		System.out.println("testReadRequest");
		Server server1 = new Server("S1", serverPort1);
		server1.start();
		assertTrue(server1.isAlive());

		ClusterManager clusterManager = ClusterManager.getInstance();
		clusterManager.addReplica("localhost", serverPort1);

		Client client = new Client("C1", "localhost", clientPort1);
		client.addReplica("localhost", serverPort1);
		client.start();
		Transaction t1 = new Transaction("T1");
		t1.addReadOperation("X");
		client.put(t1);
		// client.send("TEST DATA", "localhost", serverPort1);

		// test stopping server
		assertTrue(server1.isAlive());
		client.send("exit", "localhost", serverPort1);
		// Thread.sleep(50);
		// assertFalse(server1.isAlive());
	}

}
