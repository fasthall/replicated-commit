/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.IOException;
import java.net.UnknownHostException;

import org.codehaus.jettison.json.JSONException;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

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

	public void testReadRequest() throws UnknownHostException, IOException,
			InterruptedException, ShutdownSignalException,
			ConsumerCancelledException, JSONException {
		System.out.println("testReadRequest");
		Server dc1_0 = new Server("DC1_0", true, "dc1", 3, 3);
		Server dc1_1 = new Server("DC1_1", false, "dc1", 3, 3);
		Server dc1_2 = new Server("DC1_2", false, "dc1", 3, 3);
		Server dc2_0 = new Server("DC2_0", true, "dc2", 3, 3);
		Server dc2_1 = new Server("DC2_1", false, "dc2", 3, 3);
		Server dc2_2 = new Server("DC2_2", false, "dc2", 3, 3);
		Server dc3_0 = new Server("DC3_0", true, "dc3", 3, 3);
		Server dc3_1 = new Server("DC3_1", false, "dc3", 3, 3);
		Server dc3_2 = new Server("DC3_2", false, "dc3", 3, 3);
		dc1_0.addCoordinators("DC1_0", "DC2_0", "DC3_0");
		dc2_0.addCoordinators("DC1_0", "DC2_0", "DC3_0");
		dc3_0.addCoordinators("DC1_0", "DC2_0", "DC3_0");
		dc1_0.addOneWayLatency("client", 40);
		dc1_1.addOneWayLatency("client", 40);
		dc1_2.addOneWayLatency("client", 40);
		dc2_0.addOneWayLatency("client", 50);
		dc2_1.addOneWayLatency("client", 50);
		dc2_2.addOneWayLatency("client", 50);
		dc3_0.addOneWayLatency("client", 60);
		dc3_1.addOneWayLatency("client", 60);
		dc3_2.addOneWayLatency("client", 60);
		dc1_0.start();
		dc1_1.start();
		dc1_2.start();
		dc2_0.start();
		dc2_1.start();
		dc2_2.start();
		dc3_0.start();
		dc3_1.start();
		dc3_2.start();

		Client client1 = new Client("C1");
		client1.addReplicas("DC1_0", "DC1_1", "DC1_2", "DC2_0", "DC2_1",
				"DC2_2", "DC3_0", "DC3_1", "DC3_2");
		client1.addCoordinator("DC1_0", "DC2_0", "DC3_0");
		client1.addOneWayLatency("DC1_0", 40);
		client1.addOneWayLatency("DC1_1", 40);
		client1.addOneWayLatency("DC1_2", 40);
		client1.addOneWayLatency("DC2_0", 50);
		client1.addOneWayLatency("DC2_1", 50);
		client1.addOneWayLatency("DC2_2", 50);
		client1.addOneWayLatency("DC3_0", 60);
		client1.addOneWayLatency("DC3_1", 60);
		client1.addOneWayLatency("DC3_2", 60);
		Thread.sleep(50);
		for (int i = 0; i < 10; ++i) {
			Transaction t1 = new Transaction();
			t1.addWriteOperation("X", "v" + i);
			t1.addReadOperation("X");
			if (client1.put(t1)) {
				++App.commit;
			} else {
				++App.abort;
			}
		}
		Transaction t1 = new Transaction();
		t1.addReadOperation("X");
		client1.put(t1);
//		Thread.sleep(50);
		System.out.println();

		Transaction t2= new Transaction();
		t2.addReadOperation("X");
		client1.put(t2);
//		Thread.sleep(50);
		System.out.println();

		Transaction t3 = new Transaction();
		t3.addWriteOperation("X", "TEST");
		client1.put(t3);
		
		
		System.out.println("commit " + App.commit);
		System.out.println("abort " + App.abort);
		System.out.println(App.err4Str);

		// try {
		// dc1_0.join();
		// dc1_1.join();
		// dc1_2.join();
		// dc2_0.join();
		// dc2_1.join();
		// dc2_2.join();
		// dc3_0.join();
		// dc3_1.join();
		// dc3_2.join();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// Thread.sleep(50);
		// client1.send("exit", "localhost", serverPort1);
		// Thread.sleep(50);
		// assertFalse(server1.isAlive());
	}

}
