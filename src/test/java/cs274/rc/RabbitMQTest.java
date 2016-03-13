/*
 * @Author Wei-Tsung Lin
 * @Date 02/27/2016
 */
package cs274.rc;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

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
		Channel channel2 = connection2.createChannel();
		Connection connection3 = factory.newConnection();
		Channel channel3 = connection3.createChannel();

		channel2.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String queueName2 = channel2.queueDeclare().getQueue();
		channel2.queueBind(queueName2, EXCHANGE_NAME, "");
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		Consumer consumer2 = new DefaultConsumer(channel2) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				System.out.println(consumerTag + " [x] Received '" + message
						+ "'");
			}
		};
		channel2.basicConsume(queueName2, true, consumer2);

		channel3.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String queueName3 = channel3.queueDeclare().getQueue();
		channel3.queueBind(queueName3, EXCHANGE_NAME, "");
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		Consumer consumer3 = new DefaultConsumer(channel3) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				System.out.println(consumerTag + " [x] Received '" + message
						+ "'");
			}
		};
		channel3.basicConsume(queueName3, true, consumer3);

		Thread.sleep(500);
		String message = "Hello World!";
		channel1.exchangeDeclare(EXCHANGE_NAME, "fanout");
		channel1.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
		System.out.println(" [x] Sent '" + message + "'");

		Thread.sleep(500);
		channel1.close();
		connection1.close();
	}

}
