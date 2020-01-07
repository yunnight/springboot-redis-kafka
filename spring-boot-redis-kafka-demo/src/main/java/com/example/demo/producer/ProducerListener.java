package com.example.demo.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.example.demo.util.ApplicationContextUtil;

public class ProducerListener implements ApplicationListener<ContextRefreshedEvent> {

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		String tmp = ApplicationContextUtil.getBean(Environment.class).getProperty("kafka.consumer.thread-size");
		int threadSize = Integer.parseInt(tmp);
		tmp = ApplicationContextUtil.getBean(Environment.class).getProperty("redis.sleep-time");
		long sleepTime = Long.parseLong(tmp);
		StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
		KafkaProducer kafkaProducer = ApplicationContextUtil.getBean(KafkaProducer.class);

		ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
		for (int i = 0; i < threadSize; i++) {
			//executorService.submit(new ProducerThread(stringRedisTemplate, kafkaProducer, getBucketKey(i), sleepTime));
			executorService.submit(new ProducerLockThread(stringRedisTemplate, kafkaProducer, getBucketKey(i), sleepTime));
		}
	}

	private boolean isFirstStart() {
		return false;
	}
	
	private String getBucketKey(int i) {
		return "CYSBUCKET_" + i;
	}

}
