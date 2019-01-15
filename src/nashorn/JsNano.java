package nashorn;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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

import util.MQUtil;
import util.RedisUtil;

public class JsNano {
	public static String id = null; // requestId
	public static String projectId = null;
	public static String nanoServiceId = null;
	public static String redisHost = "redis";
	public static String redisUsername = null;
	public static String redisPassword = null;
	public static String code = "return 'Hello World!'";

	public static String rabbitHost = "rabbitmq";
	public static String rabbitUsername = "guest";
	public static String rabbitPassword = "guest";
	public static String fetchQueue = "icb-fetch";
	public static String resultQueue = "icb-result";
	public static String errorQueue = "icb-error";
	public static String callbackQueue = null;
	public static int threadCount = 1;

	public static int requestCount = 0;
	public static boolean debug = false;

	public static ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");

	public static Map paramMap = new HashMap();

	public Consumer getDefaultConsumer(Channel channel, String threadId) throws ScriptException {
		System.out.println(" [FETCH] Waiting for messages. " + threadId);

		return new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				int reqNum = ++requestCount;
				try {
					long startTime = System.currentTimeMillis();
					String message = new String(body, "UTF-8"); // scenarioId:[startLocationId:lat,lon]x[destinationLocationId:lat,lon]
					if (message != null && message.startsWith("icb-kill")) {
						channel.basicPublish("", fetchQueue, null,
								(message + ":" + threadId).toString().getBytes("UTF-8"));
						channel.basicCancel(message);
					}
					Object funcResult = ((Invocable) engine).invokeFunction("jsNanoMQ", message, paramMap);
					;
					if (debug)
						System.out.println(new Date() + " - #" + reqNum + " -> "
								+ (System.currentTimeMillis() - startTime) + "ms ; " + funcResult);
				} catch (Exception e) {
					System.out.println("MQ Error: " + e.getMessage());
				}
			}
		};
	}

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 1) {
			projectId = args[0];
			nanoServiceId = args[1];
			for (int qi = 0; qi < args.length; qi++) {
				String[] kv = args[qi].replace('=', ',').split(",");
				if (kv.length > 1) {
					if (kv[0].equals("id"))
						id = kv[1];
					else if (kv[0].equals("redis"))
						redisHost = kv[1];
					else if (kv[0].equals("rabbit"))
						rabbitHost = kv[1];
					else if (kv[0].equals("threadCount"))
						threadCount = Integer.parseInt(kv[1]);
					else if (kv[0].equals("fetchQueue"))
						fetchQueue = kv[1];
					else if (kv[0].equals("resultQueue"))
						resultQueue = kv[1];
					else if (kv[0].equals("callbackQueue"))
						callbackQueue = kv[1];
					else if (kv[0].equals("errorQueue"))
						errorQueue = kv[1];
					else if (kv[0].equals("redisUsername"))
						redisUsername = kv[1];
					else if (kv[0].equals("redisPassword"))
						redisPassword = kv[1];
					else if (kv[0].equals("rabbitUsername"))
						rabbitUsername = kv[1];
					else if (kv[0].equals("rabbitPassword"))
						rabbitPassword = kv[1];
					paramMap.put(kv[0], kv[1]);
				} else
					paramMap.put(kv[0], "1");
			}
		} else {
			throw new Exception("Missing projectId & nanoServiceId parameters");
		}

		code = RedisUtil.get(redisHost, projectId + ":" + nanoServiceId);
		if (code == null || code.length() == 0)
			throw new Exception("NanoService Code not found in Redis: [" + projectId + ":" + nanoServiceId + "]");

		debug = paramMap.containsKey("debug");

		if (!paramMap.containsKey("mq")) {
			try {
				long startTime = System.currentTimeMillis();
				if (debug)System.out.println(new Date() + " - START " + projectId + "-" + nanoServiceId + (id!=null ? "-"+id:""));
				engine.eval("function jsNano(params){\n" + code + "\n}");
				for(int ti=0;ti<20;ti++){
					long startTime2 = System.currentTimeMillis();
					
					Object funcResult = ((Invocable) engine).invokeFunction("jsNano", paramMap);
					
					System.out.println(ti + ". " + (System.currentTimeMillis() - startTime2) + "ms; Result: " + funcResult);
				}

//				for(int qi=0,j=0;qi<1000000000;qi++)j++;
				if (debug)System.out.println(new Date() + " - END " + (System.currentTimeMillis() - startTime) + "ms");
				if (callbackQueue != null && id != null) {
					MQUtil.getChannel4Queue(rabbitHost, callbackQueue).basicPublish("", callbackQueue, null,
							("icb-finish:" + id).toString().getBytes("UTF-8"));
				}
			} finally {
				try {
					MQUtil.closeAll();
				} catch (Exception e) {}

				try {
					RedisUtil.closeAll();
				} catch (Exception e) {}
			}

		} else {
			engine.eval("function jsNanoMQ(msg,params){\n" + code + "\n}");
			System.out.println("MQ thread count / host / fetch queue / result queue / error queue");
			System.out.println(
					threadCount + " / " + rabbitHost + " / " + fetchQueue + " / " + resultQueue + " / " + errorQueue);

			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(rabbitHost);
			if (rabbitUsername != null) {
				factory.setUsername(rabbitUsername);
				factory.setPassword(rabbitPassword);
			}

			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.queueDeclare(fetchQueue, false, false, false, null);
			channel.queueDeclare(resultQueue, false, false, false, null);
			channel.queueDeclare(errorQueue, false, false, false, null);

			Consumer consumer = new JsNano().getDefaultConsumer(channel, projectId + "-" + nanoServiceId + "-0");
			channel.basicConsume(fetchQueue, true, consumer);
		}

	}

}
