package util;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class RedisUtil {
	private static Map<String, Jedis> redisMap = new HashMap<>(); 
	
	public static Jedis getConnection(String host){
		Jedis j = redisMap.get(host);
		if(j!=null){
			if(!j.isConnected()){
				try{j.close();}catch(Exception ee){}				
			} else return j;
		}
		j = new Jedis(host);
		redisMap.put(host, j);
		return j;		
	}
	
	public static void closeAll(){
	}
	
	public static void close(String host){
		Jedis j = redisMap.get(host);
		if(j!=null){
			if(!j.isConnected()){
				try{j.close();}catch(Exception ee){}				
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
	
	public static String get(String host, String k){
		Jedis j = getConnection(host);
		return j.get(k);		
	}


	public static String info(String host, String section) {
		Jedis j = getConnection(host);
		return j.info(section);	
	}
	
}
