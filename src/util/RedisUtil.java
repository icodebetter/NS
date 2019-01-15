package util;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class RedisUtil {
	private static Map<String, Jedis> redisHostMap = new HashMap<>(); 
	
	private String host = null;
	private Jedis jedis = null;
	
	
	public Jedis getJedis() {
		return jedis;
	}


	public RedisUtil(String host, String userName, String passWord) {
		super();
		this.host = host;
		jedis = redisHostMap.get(host);
		if(jedis!=null){
			if(!jedis.isConnected()){
				redisHostMap.remove(host);
				try{jedis.close();}catch(Exception ee){}				
			} else return;
		}
		jedis = new Jedis(host);
		redisHostMap.put(host, jedis);
	}


	public static Jedis getConnection(String host){
		Jedis j = redisHostMap.get(host);
		if(j!=null){
			if(!j.isConnected()){
				redisHostMap.remove(host);
				try{j.close();}catch(Exception ee){}				
			} else return j;
		}
		j = new Jedis(host);
		redisHostMap.put(host, j);
		return j;		
	}
	
	
	public static void close(String host){
		Jedis j = redisHostMap.get(host);
		if(j!=null){
			if(!j.isConnected()){
				redisHostMap.remove(host);
				try{j.close();}catch(Exception ee){}				
			}
		}		
	}
	public void close(){
		if(jedis!=null){
			if(!jedis.isConnected()){
				redisHostMap.remove(host);
				try{jedis.close();}catch(Exception ee){}				
			}
		}		
	} 
	
	public static String put(String host, String k, String v){
		Jedis j = getConnection(host);
		return j.set(k, v);		
	}
	
	/*
	public static String add(String host, String k, String v){
		Jedis j = getConnection(host);
		return j.(k, v);		
	}*/
	
	public static long rpush(String host, String k, String v){
		Jedis j = getConnection(host);
		return j.rpush(k, v);		
	}
	
	public static long lpush(String host, String k, String v){
		Jedis j = getConnection(host);
		return j.lpush(k, v);		
	}
	
	
	public static String rpop(String host, String k){
		Jedis j = getConnection(host);
		return j.rpop(k);		
	}
	
	public static String lpop(String host, String k){
		Jedis j = getConnection(host);
		return j.lpop(k);		
	}
	
	public static long llen(String host, String k){
		Jedis j = getConnection(host);
		return j.llen(k);		
	}
	
	public static String get(String host, String k){
		Jedis j = getConnection(host);
		return j.get(k);		
	}


	public static String info(String host, String section) {
		Jedis j = getConnection(host);
		return j.info(section);	
	}
	
	public static void closeAll(){
		for(Jedis j:redisHostMap.values())if(j!=null && j.isConnected())try{
			j.close();
		}catch(Exception ee){}
		redisHostMap.clear();
	}
}
