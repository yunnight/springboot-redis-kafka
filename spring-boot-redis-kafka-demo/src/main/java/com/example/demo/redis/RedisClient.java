package com.example.demo.redis;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.CollectionUtils;

public class RedisClient {

	private StringRedisTemplate stringRedisTemplate;

	private static Long ttl = 10l;// 10s

	public RedisClient(StringRedisTemplate stringRedisTemplate) {
		super();
		this.stringRedisTemplate = stringRedisTemplate;
	}

	public void addKeyString(String key, String val) {
		stringRedisTemplate.opsForValue().set(key, val, ttl, TimeUnit.SECONDS);
	}

	public String getKeyString(String key) {
		return stringRedisTemplate.opsForValue().get(key);
	}

	public void delKeyString(String key) {
		stringRedisTemplate.delete(key);
	}

	public boolean addZset(String key, String val, double score) {
		return stringRedisTemplate.opsForZSet().add(key, val, score);
	}

	public String getFirstZset(String key) {
		Set<String> range = stringRedisTemplate.opsForZSet().range(key, 0l, 0l); // 取第一个元素（分数最小）
		if (CollectionUtils.isEmpty(range)) {
			return null;
		}
		return range.iterator().next();
	}
	
	public void delZsetValue(String key, String value) {
		stringRedisTemplate.opsForZSet().remove(key, value);
	}
	
	public Boolean setIfNotExist(String key, String value) {
		return stringRedisTemplate.opsForValue().setIfAbsent(key, value);
	}
	
	public String getKeyStringAndSet(String key, String value) {
		return stringRedisTemplate.opsForValue().getAndSet(key, value);
	}

}
