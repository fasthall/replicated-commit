/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.util.LinkedList;
import java.util.Queue;

import com.rabbitmq.client.QueueingConsumer.Delivery;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class DelayBufferTest extends TestCase {

	public DelayBufferTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(DelayBufferTest.class);
	}

	public void testDelayBuffer() {
		Queue<Delivery> delivered = new LinkedList<Delivery>();
		DelayBuffer delayBuffer = new DelayBuffer(delivered);
		delayBuffer.addLatency("DC1", 1000);
		delayBuffer.addLatency("DC2", 2000);
		delayBuffer.addLatency("DC3", 3000);
		Delivery delivery1 = new Delivery(null, null, "Test1".getBytes());
		Delivery delivery2 = new Delivery(null, null, "Test2".getBytes());
		Delivery delivery3 = new Delivery(null, null, "Test3".getBytes());
		delayBuffer.addToBuffer(delivery1, "DC1");
		delayBuffer.addToBuffer(delivery2, "DC2");
		delayBuffer.addToBuffer(delivery3, "DC3");
		delayBuffer.start();
		// Delivery delayedDelivery = delayBuffer.nextDelivery();
		// System.out.println(delayedDelivery.getBody().toString());

		while (true) {
			synchronized (delivered) {
				if (!delivered.isEmpty()) {
					Delivery delivery = delivered.remove();
					System.out.println(new String(delivery.getBody()));
				}
			}
		}
	}

}
