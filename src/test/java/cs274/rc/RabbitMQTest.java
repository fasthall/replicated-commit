/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class RabbitMQTest extends TestCase {

	private static final String EXCHANGE_NAME = "logs";

	public RabbitMQTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(RabbitMQTest.class);
	}

	public void testChannel() throws IOException, TimeoutException,
			InterruptedException {
		System.out.println("testChannel");
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection1 = factory.newConnection();
		Channel channel1 = connection1.createChannel();
		Connection connection2 = factory.newConnection();
		final Channel channel2 = connection2.createChannel();
		Connection connection3 = factory.newConnection();
		Channel channel3 = connection3.createChannel();

		Consumer consumer = new DefaultConsumer(channel2) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				String replyTo = properties.getReplyTo();
				String corrID = properties.getCorrelationId();
				BasicProperties replyProps = new BasicProperties.Builder()
						.correlationId(corrID).build();
				System.out.println(consumerTag + " [x] Received '" + message
						+ "'   " + replyTo);
				channel2.basicPublish("", replyTo, replyProps,
						"ACK!!".getBytes());
			}
		};
		channel2.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String queueName2 = channel2.queueDeclare().getQueue();
		channel2.queueBind(queueName2, EXCHANGE_NAME, "");
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		channel2.basicConsume(queueName2, true, consumer);

		channel3.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String queueName3 = channel3.queueDeclare().getQueue();
		channel3.queueBind(queueName3, EXCHANGE_NAME, "");
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		channel3.basicConsume(queueName3, true, consumer);

		Thread.sleep(500);
		String message = "Hello World!";
		channel1.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String queueName1 = channel1.queueDeclare().getQueue();
		String corrID = UUID.randomUUID().toString();
		String replyQueueName = channel1.queueDeclare().getQueue();
		BasicProperties props = new BasicProperties.Builder()
				.correlationId(corrID).replyTo(replyQueueName).build();
		QueueingConsumer qConsumer = new QueueingConsumer(channel1);
		channel1.queueBind(queueName1, EXCHANGE_NAME, "");
		channel1.basicConsume(queueName1, true, consumer);
		channel1.basicConsume(replyQueueName, true, qConsumer);
		channel1.basicPublish(EXCHANGE_NAME, "", props, message.getBytes());
		System.out.println(" [x] Sent '" + message + "'");
		int cnt = 0;
		while (cnt < 3) {
			Delivery delivery = qConsumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrID)) {
				System.out.println(new String(delivery.getBody()));
				++cnt;
			}
		}

		channel1.close();
		connection1.close();
	}
}
