package com.example.demo.producer;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import com.example.demo.redis.RedisClient;
import com.example.demo.redis.RedisLock;

import redis.clients.jedis.Jedis;

public class ProducerLockThread implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(ProducerLockThread.class);

	private StringRedisTemplate stringRedisTemplate;

	private RedisClient redisClient;

	private KafkaProducer kafkaProducer;

	private String bucketKey;

	private long sleepTime;

	public ProducerLockThread(StringRedisTemplate stringRedisTemplate, KafkaProducer kafkaProducer, String bucketKey, long sleepTime) {
		super();
		this.stringRedisTemplate = stringRedisTemplate;
		this.redisClient = new RedisClient(stringRedisTemplate);
		this.kafkaProducer = kafkaProducer;
		this.bucketKey = bucketKey;
		this.sleepTime = sleepTime;
	}

	@Override
	public void run() {
		try {
			String lockKey = getBucketLockKey(bucketKey);
			while (true) {
				RedisLock lock = new RedisLock(redisClient, lockKey);
				try{
					String val = redisClient.getFirstZset(bucketKey);
					if (StringUtils.isEmpty(val)) {
						sleep();
						continue;
					}
					String[] tmp = val.split("@@");
					String mId = tmp[0];
					String delayTime = tmp[1];

					logger.info("取出Redis------mId: {}, delayTime: {}, bucketKey: {}", mId, delayTime, bucketKey);
					if (lock.acquire()) {
						logger.info("Get lock------lockKey: {}", lockKey);

						// 时间未到
						if (Long.parseLong(delayTime) > System.currentTimeMillis()) {
							sleep();
							continue;
						}

						String redisMsg = redisClient.getKeyString(mId);
						if (StringUtils.isEmpty(redisMsg)) {
							sleep();
							redisClient.delZsetValue(bucketKey, val);
							continue;
						}

						tmp = redisMsg.split("@@");
						String message = tmp[0];
						logger.info("取出Redis------mId: {}, delayTime: {}, message: {}", mId, delayTime, message);

						// kafka生产
						kafkaProducer.send(mId, message);
						logger.info("生产kafka消息------mId: {}, message: {}", mId, message);

						redisClient.delZsetValue(bucketKey, val);
						logger.info("Redis Zset删除------key: {}, val: {}", bucketKey, val);
					}
				}catch (Exception e) {
					e.printStackTrace();
				}finally{
					lock.release();
					//logger.info("Release lock------lockKey: {}", lockKey);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private String getBucketLockKey(String buckeyKey) {
		return buckeyKey + "_lock";
	}

	private void sleep() {
		if (sleepTime > 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
