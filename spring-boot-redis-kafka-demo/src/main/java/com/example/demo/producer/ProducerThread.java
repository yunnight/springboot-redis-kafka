package com.example.demo.producer;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import com.example.demo.redis.RedisClient;

public class ProducerThread implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(ProducerThread.class);

	private StringRedisTemplate stringRedisTemplate;

	private RedisClient redisClient;

	private KafkaProducer kafkaProducer;

	private String bucketKey;

	private long sleepTime;

	public ProducerThread(StringRedisTemplate stringRedisTemplate, KafkaProducer kafkaProducer, String bucketKey, long sleepTime) {
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
			while (true) {
				try {
					String val = redisClient.getFirstZset(bucketKey);
					if (StringUtils.isEmpty(val)) {
						sleep();
						continue;
					}
					String[] tmp = val.split("@@");
					String mId = tmp[0];
					String delayTime = tmp[1];

					logger.info("取出Redis------mId: {}, delayTime: {}, bucketKey: {}", mId, delayTime, bucketKey);

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
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

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
