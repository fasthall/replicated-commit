/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.IOException;
import java.net.UnknownHostException;

import cs274.rc.connection.ClusterManager;
import cs274.rc.connection.Node;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CommunicationTest extends TestCase {

	private int serverPort1 = 30001;
	private int serverPort2 = 30002;
	private int serverPort3 = 30003;
	private int clientPort1 = 40001;

	public CommunicationTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(CommunicationTest.class);
	}

	public void testReadRequest() throws UnknownHostException, IOException, InterruptedException {
		System.out.println("testReadRequest");
		Server server1 = new Server("S1", "localhost", serverPort1, true);
		Server server2 = new Server("S2", "localhost", serverPort2, true);
		server1.addOtherCoordinator(new Node("localhost", serverPort2, true));
		server2.addOtherCoordinator(new Node("localhost", serverPort1, true));
		// Server server3 = new Server("S3", serverPort3);
		server1.start();
		server2.start();
		// server3.start();

		ClusterManager clusterManager = new ClusterManager();
		clusterManager.addReplica("localhost", serverPort1, true);
		clusterManager.addReplica("localhost", serverPort2, true);
		// clusterManager.addReplica("localhost", serverPort3);

		Client client1 = new Client("C1", "localhost", clientPort1, clusterManager);
		client1.start();
		Thread.sleep(100);
		Transaction t1 = new Transaction("T1");
		t1.addWriteOperation("X", "v1");
		t1.addWriteOperation("Y", "v2");
		client1.put(t1);

		Transaction t2 = new Transaction("T2");
		t2.addReadOperation("X");
		t2.addReadOperation("Y");
		client1.put(t2);

		// test stopping server
		assertTrue(server1.isAlive());
		Thread.sleep(50);
		client1.send("exit", "localhost", serverPort1);
		// Thread.sleep(50);
		// assertFalse(server1.isAlive());
	}

}
