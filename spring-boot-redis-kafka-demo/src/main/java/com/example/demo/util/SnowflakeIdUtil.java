package com.example.demo.util;

public class SnowflakeIdUtil {
	
	private static SnowflakeIdWorker snowflakeIdWorker;

	public SnowflakeIdUtil(long workerId, long datacenterId) {
		snowflakeIdWorker = new SnowflakeIdWorker(workerId, datacenterId);
	}
	
	public long nextId(){
		return snowflakeIdWorker.nextId();
	}

}
