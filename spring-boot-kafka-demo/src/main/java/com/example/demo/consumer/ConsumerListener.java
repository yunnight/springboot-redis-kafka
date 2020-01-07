package com.example.demo.consumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import com.example.demo.util.ApplicationContextUtil;

import kafka.consumer.KafkaStream;

public class ConsumerListener implements ApplicationListener<ContextRefreshedEvent> {

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		int threadSize = 3;

		ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
		KafkaConsumer kafkaConsumer = ApplicationContextUtil.getBean(KafkaConsumer.class);
		List<KafkaStream<byte[], byte[]>> kafkaStreamList = kafkaConsumer.getKafkaStreamList();
		for (KafkaStream<byte[], byte[]> stream : kafkaStreamList) {
			executorService.submit(new ConsumerThread(stream));
		}
	}

	private boolean isFirstStart() {
		return false;
	}

}
