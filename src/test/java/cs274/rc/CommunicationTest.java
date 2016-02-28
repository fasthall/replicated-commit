/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.IOException;
import java.net.UnknownHostException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CommunicationTest extends TestCase {

	public CommunicationTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(CommunicationTest.class);
	}

	public void testClientToServer() throws UnknownHostException, IOException, InterruptedException {
		System.out.println("testClientToServer");
		Server server = new Server("DC1", 30001);
		server.start();
		assertTrue(server.isAlive());
		Client client = new Client();
		client.send("TEST DATA", "localhost", 30001);
		assertTrue(server.isAlive());
		client.send("exit", "localhost", 30001);
		Thread.sleep(500);
		assertFalse(server.isAlive());
	}

}
