/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

import cs274.rc.datastore.AbstractDatastore;
import cs274.rc.datastore.AbstractLog;
import cs274.rc.datastore.DatastoreEntry;
import cs274.rc.datastore.LogEntry;

public class Server extends Thread {

	private boolean coordinator;
	private String name;
	private int shardNum;
	private int coordinatorNum;
	private LockManager lockManager;
	private AbstractDatastore datastore;
	private AbstractLog abstractLog;

	private Channel channel;
	private QueueingConsumer paxosQueueingConsumer;
	private QueueingConsumer replyQueueingConsumer;
	private String readQueue;
	private String paxosQueue;
	private String replyQueue;
	private String shardExchange;

	public Server(String name, boolean coordinator, String shardExchange, int shardNum, int coordinatorNum) {
		this.name = name;
		this.coordinator = coordinator;
		this.shardExchange = shardExchange;
		this.shardNum = shardNum;
		this.coordinatorNum = coordinatorNum;
		lockManager = new LockManager();
		datastore = new AbstractDatastore();
		abstractLog = new AbstractLog(datastore);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try {
			Connection connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {
			if (coordinator) {
				startCoordinator();
			} else {
				startCohort();
			}
		} catch (ShutdownSignalException | ConsumerCancelledException | IOException | InterruptedException
				| JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void startCoordinator() throws IOException, ShutdownSignalException, ConsumerCancelledException,
			InterruptedException, JSONException {
		Consumer readConsumer = new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body, "UTF-8");
				String replyTo = properties.getReplyTo();
				String corrID = properties.getCorrelationId();
				try {
					JSONObject json = new JSONObject(message);
					int action = json.getInt("action");
					if (action == Communication.READ_REQUEST) {
						handleRead(json.getString("key"), json.getString("transaction"), corrID, replyTo);
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
				System.out.println(name + " received '" + message + "'");
			}
		};
		channel.exchangeDeclare(Communication.EXCHANGE_COORDINATORS, "fanout");
		channel.exchangeDeclare(Communication.EXCHANGE_REPLICAS, "fanout");
		channel.exchangeDeclare(shardExchange, "fanout");
		readQueue = channel.queueDeclare().getQueue();
		paxosQueue = channel.queueDeclare().getQueue();
		replyQueue = channel.queueDeclare().getQueue();
		paxosQueueingConsumer = new QueueingConsumer(channel);
		replyQueueingConsumer = new QueueingConsumer(channel);
		channel.queueBind(readQueue, Communication.EXCHANGE_REPLICAS, "");
		channel.basicConsume(readQueue, true, readConsumer);
		channel.queueBind(paxosQueue, Communication.EXCHANGE_COORDINATORS, "");
		channel.basicConsume(paxosQueue, true, paxosQueueingConsumer);
		channel.basicConsume(replyQueue, true, replyQueueingConsumer);

		while (true) {
			Delivery delivery = paxosQueueingConsumer.nextDelivery();
			JSONObject json = new JSONObject(new String(delivery.getBody()));
			int action = json.getInt("action");
			String corrID = delivery.getProperties().getCorrelationId();
			String replyTo = delivery.getProperties().getReplyTo();
			switch (action) {
			case Communication.PAXOS_REQUEST:
				handlePaxosRequest(json.getString("buffer"), json.getString("transaction"), json.getLong("version"),
						corrID, replyTo);
				break;
			case Communication.TPC_PREPARE:
				handle2PCPrepare(json.getString("buffer").split(","), json.getString("transaction"), corrID, replyTo);
				break;
			case Communication.TPC_COMMIT:
				handle2PCCommit(json.getLong("version"), json.getString("transaction"));
				break;
			}
		}
	}

	public void startCohort() throws IOException, ShutdownSignalException, ConsumerCancelledException,
			InterruptedException, JSONException {
		Consumer readConsumer = new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body, "UTF-8");
				String replyTo = properties.getReplyTo();
				String corrID = properties.getCorrelationId();
				try {
					JSONObject json = new JSONObject(message);
					int action = json.getInt("action");
					if (action == Communication.READ_REQUEST) {
						handleRead(json.getString("key"), json.getString("transaction"), corrID, replyTo);
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
				System.out.println(name + " received '" + message + "'");
			}
		};
		channel.exchangeDeclare(Communication.EXCHANGE_REPLICAS, "fanout");
		channel.exchangeDeclare(shardExchange, "fanout");
		readQueue = channel.queueDeclare().getQueue();
		String tpcQueue = channel.queueDeclare().getQueue();
		;
		channel.queueBind(readQueue, Communication.EXCHANGE_REPLICAS, "");
		channel.queueBind(tpcQueue, shardExchange, "");
		channel.basicConsume(readQueue, true, readConsumer);
		QueueingConsumer tpcConsumer = new QueueingConsumer(channel);
		channel.basicConsume(tpcQueue, true, tpcConsumer);

		while (true) {
			Delivery delivery = tpcConsumer.nextDelivery();
			JSONObject json = new JSONObject(new String(delivery.getBody()));
			int action = json.getInt("action");
			String corrID = delivery.getProperties().getCorrelationId();
			String replyTo = delivery.getProperties().getReplyTo();
			switch (action) {
			case Communication.TPC_PREPARE:
				handle2PCPrepare(json.getString("buffer").split(","), json.getString("transaction"), corrID, replyTo);
				break;
			case Communication.TPC_COMMIT:
				handle2PCCommit(json.getLong("version"), json.getString("transaction"));
				break;
			}
		}
	}

	public String getValue(String key) {
		if (datastore.get(key) == null)
			return "null";
		return datastore.get(key).getValue();
	}

	private void handleRead(String key, String transaction, String corrID, String replyTo)
			throws NumberFormatException, UnknownHostException, IOException, JSONException {
		// set the shared lock
		if (lockManager.setShared(key, transaction)) {
			// successfully set, return the latest version
			DatastoreEntry entry = datastore.get(key);
			String value = (entry == null ? "NULL" : entry.getValue());
			long version = (entry == null ? 0 : entry.getVersion());
			JSONObject json = new JSONObject();
			json.put("action", Communication.READ_ACCEPT);
			json.put("value", value);
			json.put("version", version);
			BasicProperties replyProps = new BasicProperties.Builder().correlationId(corrID).build();
			channel.basicPublish("", replyTo, replyProps, json.toString().getBytes());
		} else {
			System.out.println("Cannot set " + key + " for " + transaction);
			// can't acquire the lock
			JSONObject json = new JSONObject();
			json.put("action", Communication.READ_REJECT);
			BasicProperties replyProps = new BasicProperties.Builder().correlationId(corrID).build();
			channel.basicPublish("", replyTo, replyProps, json.toString().getBytes());
		}
	}

	private void handlePaxosRequest(String writeBuffer, String transaction, long version, String clientCorrID,
			String clientReplyTo) throws UnknownHostException, IOException, JSONException, ShutdownSignalException,
					ConsumerCancelledException, InterruptedException {
		// send 2PC prepare to cohorts and self
		TPCPool tpcPool = new TPCPool();

		// wait until all reply
		JSONObject tpcJson = new JSONObject();
		tpcJson.put("action", Communication.TPC_PREPARE);
		tpcJson.put("buffer", writeBuffer);
		tpcJson.put("transaction", transaction);
		String tpcCorrID = UUID.randomUUID().toString();
		BasicProperties props = new BasicProperties.Builder().correlationId(tpcCorrID).replyTo(replyQueue).build();
		channel.basicPublish(shardExchange, "", props, tpcJson.toString().getBytes());
		if (handle2PCPrepareSelf(writeBuffer.split(","), transaction)) {
			tpcPool.addAccept();
		} else {
			tpcPool.addReject();
		}

		while (tpcPool.getAcceptCount() + tpcPool.getRejectCount() < shardNum) {
			// Wait for all shards
			Delivery delivery = replyQueueingConsumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(tpcCorrID)) {
				JSONObject json = new JSONObject(new String(delivery.getBody()));
				if (json.getInt("action") == Communication.TPC_ACCEPT) {
					tpcPool.addAccept();
				} else if (json.getInt("action") == Communication.TPC_REJECT) {
					tpcPool.addReject();
				}
			}
		}

		boolean tpcResult;
		JSONObject paxosReplyJson = new JSONObject();
		if (tpcPool.getRejectCount() == 0) {
			tpcResult = true;
			paxosReplyJson.put("action", Communication.PAXOS_ACCEPT);
		} else {
			tpcResult = false;
			paxosReplyJson.put("action", Communication.PAXOS_REJECT);
		}
		BasicProperties replyProps = new BasicProperties.Builder().correlationId(clientCorrID).build();
		channel.basicPublish("", clientReplyTo, replyProps, paxosReplyJson.toString().getBytes());

		PaxosPool paxosPool = new PaxosPool(transaction);
		if (tpcResult) {
			paxosPool.addAccept();
		} else {
			paxosPool.addReject();
		}
		channel.basicPublish(Communication.EXCHANGE_COORDINATORS, "", replyProps, paxosReplyJson.toString().getBytes());
		while (paxosPool.getAcceptCount() + paxosPool.getRejectCount() < coordinatorNum
				&& paxosPool.getAcceptCount() <= coordinatorNum / 2) {
			// Wait for majority
			Delivery delivery = paxosQueueingConsumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(clientCorrID)) {
				JSONObject json = new JSONObject(new String(delivery.getBody()));
				int action = json.getInt("action");
				if (action == Communication.PAXOS_ACCEPT) {
					paxosPool.addAccept();
				} else if (action == Communication.PAXOS_REJECT) {
					paxosPool.addReject();
				}
			}
		}
		System.out.println("Server " + name + " received from majority, start 2PC commit.");

		/*
		 * Send 2PC commit. cohorts who receive this should log commit and
		 * release locks
		 */
		if (paxosPool.getAcceptCount() > coordinatorNum / 2) {
			JSONObject json = new JSONObject();
			json.put("action", Communication.TPC_COMMIT);
			json.put("version", version);
			json.put("transaction", transaction);
			channel.basicPublish(shardExchange, "", null, json.toString().getBytes());
			handle2PCCommit(version, transaction);
		}
	}

	private void handle2PCPrepare(String[] writeBuffer, String transaction, String corrID, String replyTo)
			throws UnknownHostException, IOException, JSONException {
		// acquire all the exclusive locks
		List<String> keys = new ArrayList<String>();
		for (String writeOp : writeBuffer) {
			keys.add(writeOp.split(":")[0]);
		}
		if (lockManager.testExclusive(keys)) {
			LogEntry logEntry = new LogEntry(transaction, LogEntry.TPC_PREPARE);
			for (String writeOp : writeBuffer) {
				String key = writeOp.split(":")[0];
				String value = writeOp.split(":")[1];
				logEntry.addWrite(key, value);
				lockManager.setExclusive(key, transaction);
			}
			lockManager.unlockAllExclusiveByTransaction(transaction);
			abstractLog.put(logEntry);
			System.out.println("Server " + name + " locks: log 2PC prepare.");
			JSONObject json = new JSONObject();
			json.put("action", Communication.TPC_ACCEPT);
			BasicProperties props = new BasicProperties.Builder().correlationId(corrID).build();
			channel.basicPublish("", replyTo, props, json.toString().getBytes());
		} else {
			lockManager.unlockAllExclusiveByTransaction(transaction);
			JSONObject json = new JSONObject();
			json.put("action", Communication.TPC_REJECT);
			BasicProperties props = new BasicProperties.Builder().correlationId(corrID).build();
			channel.basicPublish("", replyTo, props, json.toString().getBytes());
		}
	}

	private boolean handle2PCPrepareSelf(String[] writeBuffer, String transaction) {
		// acquire all the exclusive locks
		List<String> keys = new ArrayList<String>();
		for (String writeOp : writeBuffer) {
			keys.add(writeOp.split(":")[0]);
		}
		if (lockManager.testExclusive(keys)) {
			LogEntry logEntry = new LogEntry(transaction, LogEntry.TPC_PREPARE);
			for (String writeOp : writeBuffer) {
				String key = writeOp.split(":")[0];
				String value = writeOp.split(":")[1];
				logEntry.addWrite(key, value);
				lockManager.setExclusive(key, transaction);
			}
			lockManager.unlockAllExclusiveByTransaction(transaction);
			abstractLog.put(logEntry);
			System.out.println("Server " + name + " locks: log 2PC prepare.");
			return true;
		} else {
			lockManager.unlockAllExclusiveByTransaction(transaction);
			return false;
		}
	}

	private void handle2PCCommit(long version, String transaction) {
		abstractLog.commit(version, transaction);
		lockManager.unlockAllExclusiveByTransaction(transaction);
		System.out.println("Server " + name + " committed locally and released the locks.");
	}

}
