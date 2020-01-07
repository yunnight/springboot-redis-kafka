package com.example.demo.consumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.example.demo.util.ApplicationContextUtil;

import kafka.consumer.KafkaStream;

public class ConsumerListener implements ApplicationListener<ContextRefreshedEvent> {

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		String tmp = ApplicationContextUtil.getBean(Environment.class).getProperty("kafka.consumer.thread-size");
		int threadSize = Integer.parseInt(tmp);
		String intervals = ApplicationContextUtil.getBean(Environment.class).getProperty("notify.interval");
		StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
		
		ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
		KafkaConsumer kafkaConsumer = ApplicationContextUtil.getBean(KafkaConsumer.class);
		List<KafkaStream<byte[], byte[]>> kafkaStreamList = kafkaConsumer.getKafkaStreamList();
		for (KafkaStream<byte[], byte[]> stream : kafkaStreamList) {
			executorService.submit(new ConsumerThread(stream, stringRedisTemplate, intervals, threadSize));
		}
	}

	private boolean isFirstStart() {
		return false;
	}

}
