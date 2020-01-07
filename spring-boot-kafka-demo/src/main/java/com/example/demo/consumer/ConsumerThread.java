package com.example.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class ConsumerThread implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

	private KafkaStream<byte[], byte[]> stream;

	public ConsumerThread(KafkaStream<byte[], byte[]> stream) {
		super();
		this.stream = stream;
	}

	@Override
	public void run() {
		try {
			ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
			while (iterator.hasNext()) {
				MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
				byte[] message = (null == messageAndMetadata.message()) ? new byte[0] : messageAndMetadata.message();
				String kafkaKey = (null == messageAndMetadata.key()) ? "" : new String(messageAndMetadata.key());

				logger.info("kafka消费------current thread: {}, partition:{}, offset:{}, kafkaKey:{}, topic:{}, message:{}", 
						Thread.currentThread().getName(), 
						messageAndMetadata.partition(), 
						messageAndMetadata.offset(), 
						kafkaKey, 
						messageAndMetadata.topic(), 
						new String(message)
						);

				String payLoad = null;
				StringDecoder decoder = new StringDecoder(new VerifiableProperties());
				payLoad = decoder.fromBytes(message);
				if (!StringUtils.isEmpty(payLoad)) {
					logger.info("打印消费信息: " + payLoad);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
