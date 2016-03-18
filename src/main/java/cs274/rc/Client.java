package cs274.rc;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import cs274.rc.protocol.PaxosPool;
import cs274.rc.protocol.ReadingPool;

import com.rabbitmq.client.ShutdownSignalException;

public class Client {

	private String name;

	private Channel channel;
	private QueueingConsumer queueingConsumer;
	private String replyQueue;
	private List<String> replicas;
	private List<String> coordinators;
	private HashMap<String, Integer> oneWayLatency;

	public Client(String name) {
		this.name = name;
		replicas = new ArrayList<String>();
		coordinators = new ArrayList<String>();
		oneWayLatency = new HashMap<String, Integer>();

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try {
			Connection connection = factory.newConnection();
			channel = connection.createChannel();
			channel.exchangeDeclare(Communication.EXCHANGE_COORDINATORS, "direct");
			channel.exchangeDeclare(Communication.EXCHANGE_REPLICAS, "direct");
			queueingConsumer = new QueueingConsumer(channel);
			replyQueue = channel.queueDeclare().getQueue();
			channel.basicConsume(replyQueue, true, queueingConsumer);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void addReplicas(String... names) {
		for (String n : names) {
			replicas.add(n);
		}
	}

	public void addCoordinator(String... names) {
		for (String n : names) {
			coordinators.add(n);
		}
	}

	public void addOneWayLatency(String to, int latency) {
		oneWayLatency.put(to, latency);
	}

	public boolean put(Transaction transaction) throws UnknownHostException, IOException, ShutdownSignalException,
			ConsumerCancelledException, JSONException, InterruptedException {
		boolean result = false;
		List<Operation> writeBuffer = new ArrayList<Operation>();
		while (true) {
			Operation operation = transaction.popOperation();
			if (operation == null) {
				// transaction terminates, start Paxos
				System.out.println("transaction terminates, start Paxos");
				result = sendPaxosRequest(transaction, writeBuffer);
				break;
			} else if (operation.getAction() == Operation.READ) {
				boolean readResult = sendReadRequest(transaction, operation);
				if (!readResult) {
					writeBuffer.clear();
					sendPaxosRequest(transaction, writeBuffer);
					return readResult;
				}
			} else if (operation.getAction() == Operation.WRITE) {
				// buffer write
				writeBuffer.add(operation);
			}
		}
		return result;
	}

	private boolean sendReadRequest(Transaction transaction, Operation operation) throws UnknownHostException,
			IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException, JSONException {
		boolean result = true;
		ReadingPool readingPool = new ReadingPool(transaction.getName(), operation.getKey());
		final JSONObject readJson = new JSONObject();
		readJson.put("action", Communication.READ_REQUEST);
		readJson.put("transaction", transaction.getName());
		readJson.put("key", operation.getKey());

		// Send read request to all replicas
		String corrID = UUID.randomUUID().toString();
		final BasicProperties props = new BasicProperties.Builder().correlationId(corrID).replyTo(replyQueue).build();
		for (String replica : replicas) {
			delayedPublish(replica, Communication.EXCHANGE_REPLICAS, replica, props, readJson.toString().getBytes());
		}

		while (readingPool.getSize() <= replicas.size() / 2
				&& readingPool.getSize() + readingPool.getReject() < replicas.size()) {
			// Waiting data from majority
			Delivery delivery = queueingConsumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrID)) {
				JSONObject json = new JSONObject(new String(delivery.getBody()));
				if (json.getInt("action") == Communication.READ_ACCEPT) {
					String value = json.getString("value");
					long version = json.getLong("version");
					readingPool.addDataFromReplica(value, version);
				} else if (json.getInt("action") == Communication.READ_REJECT) {
					readingPool.addReject();
				}
			}
		}
		if (readingPool.getSize() <= replicas.size() / 2) {
			result = false;
			System.out.println("Read " + operation.getKey() + " aborts.");
		} else {
			String value = readingPool.getMostRecentValue();
			System.out.println("Most recent data of " + operation.getKey() + " is " + value);
		}
		readingPool = null;
		return result;
	}

	private boolean sendPaxosRequest(Transaction transaction, List<Operation> writeBuffer)
			throws JSONException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		PaxosPool paxosPool = new PaxosPool(transaction.getName());
		JSONObject paxosJson = new JSONObject();
		paxosJson.put("action", Communication.PAXOS_REQUEST);
		paxosJson.put("transaction", transaction.getName());
		paxosJson.put("buffer", getJSONArray(writeBuffer));
		paxosJson.put("version", System.currentTimeMillis());

		// Send Paxos accept request to all the coordinators
		String corrID = UUID.randomUUID().toString();
		BasicProperties props = new BasicProperties.Builder().correlationId(corrID).replyTo(replyQueue).build();
		for (String coordinator : coordinators) {
			delayedPublish(coordinator, Communication.EXCHANGE_COORDINATORS, coordinator, props,
					paxosJson.toString().getBytes());
		}

		while (paxosPool.getAcceptCount() + paxosPool.getRejectCount() < coordinators.size()
				|| paxosPool.getAcceptCount() <= coordinators.size() / 2) {
			// wait for majority
			Delivery delivery = queueingConsumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrID)) {
				JSONObject json = new JSONObject(new String(delivery.getBody()));
				if (json.getInt("action") == Communication.PAXOS_ACCEPT) {
					paxosPool.addAccept();
				} else if (json.getInt("action") == Communication.PAXOS_REJECT) {
					paxosPool.addReject();
				}
			}
		}

		boolean result;
		if (paxosPool.getAcceptCount() > coordinators.size() / 2) {
			// commit success from client's view
			System.out.println("Client " + name + " successfully commits " + transaction.getName() + ".");
			result = true;
		} else {
			// abort
			System.out.println("Not enough accepts, abort.");
			result = false;
		}
		return result;
	}

	public JSONArray getJSONArray(List<Operation> writeBuffer) throws JSONException {
		JSONArray jsonArray = new JSONArray();
		if (writeBuffer.isEmpty())
			return jsonArray;
		for (Operation operation : writeBuffer) {
			JSONObject json = new JSONObject();
			json.put("key", operation.getKey());
			json.put("value", operation.getValue());
			jsonArray.put(json);
		}
		return jsonArray;
	}

	public void delayedPublish(String to, final String exchange, final String routing, final BasicProperties props,
			final byte[] body) {
		final Integer delay = oneWayLatency.get(to);
		new Thread() {
			@Override
			public void run() {
				try {
					Thread.sleep(delay == null ? 0 : delay);
					channel.basicPublish(exchange, routing, props, body);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();

	}
}
