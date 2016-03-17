package cs274.rc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import com.rabbitmq.client.QueueingConsumer.Delivery;

public class DelayBuffer extends Thread {

	private Queue<DelayedDelivery> buffered;
	private Queue<Delivery> delivered;
	private HashMap<String, Integer> latency;

	public DelayBuffer(Queue<Delivery> delivered) {
		buffered = new LinkedList<DelayedDelivery>();
		this.delivered = delivered;
		latency = new HashMap<String, Integer>();
	}

	public void addLatency(String from, int delay) {
		latency.put(from, delay);
	}

	@Override
	public synchronized void run() {
		while (true) {
			// System.out.println(buffered.size() + "");
			if (!buffered.isEmpty()) {
				// System.out.println(delivered.size());
				DelayedDelivery delayedDelivery = buffered.peek();
				String from = delayedDelivery.from;
				if (System.currentTimeMillis() > delayedDelivery.arrivedTime + latency.get(from)) {
					synchronized (delivered) {
						delivered.add(delayedDelivery.delivery);
					}
					buffered.remove();
				}
			} else {
				break;
			}
		}
	}

	public void addToBuffer(Delivery delivery, String from) {
		DelayedDelivery delayedDelivery = new DelayedDelivery(delivery, from, System.currentTimeMillis());
		buffered.add(delayedDelivery);
	}

	private class DelayedDelivery {

		public Delivery delivery;
		public String from;
		public long arrivedTime;

		public DelayedDelivery(Delivery delivery, String from, long arrivedTime) {
			this.delivery = delivery;
			this.from = from;
			this.arrivedTime = arrivedTime;
		}

	}

}
