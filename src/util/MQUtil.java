package util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;



public class MQUtil {
	private static Map<String, Connection> connMap = new HashMap<String, Connection>();
	private static Map<String, Channel> channelMap = new HashMap<String, Channel>();
	private static Map<String, DeclareOk> queueMap = new HashMap<String, DeclareOk>();
	
	public static Connection getConnection(String host){
		Connection conn = connMap.get(host);
		if(conn!=null){
			if(conn.isOpen())return conn;
			connMap.remove(host);
			try {conn.close();} 
			catch (Exception e) {}
		}
		
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(host);
	    try {
			conn = factory.newConnection();
		} catch (Exception e) {
//			if(FrameworkSetting.debug)e.printStackTrace();
			return null;
		}
	    connMap.put(host, conn);
		return conn;
	}
	
	public static void close(String host, String queueName){
		if(queueName!=null) {
			String key = host + queueName;
			Channel channel = channelMap.get(key);
			if(channel!=null){
				if(channel.isOpen())try {channel.close();} 
				catch (Exception e) {}
				connMap.remove(key);
			}
				
		}
		
		Connection conn = connMap.get(host);
		if(conn!=null){
			if(conn.isOpen())try {conn.close();} 
			catch (Exception e) {}
			connMap.remove(host);


		}

	}
	
	
	public static Channel getChannel4Queue(String host, String queueName){
		String key = host + queueName;
		Channel channel = channelMap.get(key);
		if(channel!=null){
			if(channel.isOpen())return channel;
			connMap.remove(key);
			try {channel.close();} 
			catch (Exception e) {}
		}
		Connection conn = getConnection(host);
		if(conn!=null)try{
			channel = conn.createChannel();
			DeclareOk dok = channel.queueDeclare(queueName, false, false, false, null);
			channelMap.put(key, channel);
			queueMap.put(key, dok);
		    return channel;
		}catch (Exception e) {
//			if(FrameworkSetting.debug)e.printStackTrace();
		}
		return null;
	}

	public static int getQueueMsgCount(String host, String queueName){
		String key = host + queueName;
		DeclareOk dok = queueMap.get(key);
		if(dok!=null){
			return dok.getMessageCount();
		}
		return 0;
	}
	
}
