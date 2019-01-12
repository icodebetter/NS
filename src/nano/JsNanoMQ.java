package nano;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class JsNanoMQ {
	public static String host = "35.239.184.252";
	public static String fetchQueueName = "icb-fetch";
	public static String resultQueueName = "icb-result";
	public static String errorQueueName = "icb-error";
	public static int threadCount = 20;
	public static int retryCount = 5;
	public static int successCount = 0;
	public static int errorCount = 0;
	public static int requestCount = 0;
	public static long totalTime = 0;
	public static long restTime = 0;
	public static String code= "return 'Hello World!'";
	public static ScriptEngine engine =  new ScriptEngineManager().getEngineByName("nashorn");
	
	public Consumer getDefaultConsumer(Channel channel, String threadId) throws ScriptException {
		System.out.println(" [FETCH] Waiting for messages. " + threadId);
		
		return new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				requestCount++;
				int reqNum = requestCount;
				try {
					long startTime = System.currentTimeMillis();
					String message = new String(body, "UTF-8"); //scenarioId:[startLocationId:lat,lon]x[destinationLocationId:lat,lon]
					Object funcResult = ((Invocable) engine).invokeFunction("jsNanoMQ", message);;
					
				} catch (Exception e) {
					System.out.println("MQ Error: " + e.getMessage());
				}

			}
		};
	}
					
	public static void main(String[] args) throws IOException, TimeoutException, ScriptException {
		if (args != null && args.length > 0) {
			threadCount = new Integer(args[0]);
			if (args.length > 1) {
				host = args[1];
			}
			if (args.length > 2) {
				fetchQueueName = args[2];
			}
			if (args.length > 3) {
				resultQueueName = args[3];
			}
			if (args.length > 4) {
				errorQueueName = args[4];
			}
		}
		System.out.println("MQ thread count / host / fetch queue / result queue / error queue");
		System.out.println(
				threadCount + " / " + host + " / " + fetchQueueName + " / " + resultQueueName + " / " + errorQueueName);
		
		engine.eval("function jsNanoMQ(msg){\n"+code+"\n}");

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setUsername("guest");factory.setPassword("guest");
		
		
		
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(fetchQueueName, false, false, false, null);
		channel.queueDeclare(resultQueueName, false, false, false, null);
		channel.queueDeclare(errorQueueName, false, false, false, null);

		Consumer consumer = new JsNanoMQ().getDefaultConsumer(channel, "thread-0");
		channel.basicConsume(fetchQueueName, true, consumer);

	}

}
