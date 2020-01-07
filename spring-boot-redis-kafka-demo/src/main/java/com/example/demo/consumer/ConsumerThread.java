package com.example.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import com.example.demo.redis.RedisClient;
import com.example.demo.util.SnowflakeIdUtil;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class ConsumerThread implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
	
	private static SnowflakeIdUtil idUtil = new SnowflakeIdUtil(1l, 1l);
	
	private KafkaStream<byte[], byte[]> stream;
	
	private StringRedisTemplate stringRedisTemplate;
	
	private String[] interval;
	
	private int threadSize;

	public ConsumerThread(KafkaStream<byte[], byte[]> stream, StringRedisTemplate stringRedisTemplate, String interval, int threadSize) {
		super();
		this.stream = stream;
		this.stringRedisTemplate = stringRedisTemplate;
		this.interval = interval.split(",");
		this.threadSize = threadSize;
	}

	@Override
	public void run() {
		try {
			ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
			while (iterator.hasNext()) {
				MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
				byte[] message = (null == messageAndMetadata.message()) ? new byte[0] : messageAndMetadata.message();
				String kafkaKey = (null == messageAndMetadata.key()) ? "" : new String(messageAndMetadata.key());

				logger.info("kafka消费------current thread: {}, partition: {}, offset: {}, kafkaKey: {}, topic: {}, message: {}", 
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
				if (StringUtils.isEmpty(payLoad)) {
					throw new Exception("payLoad is null");
				}
				
				RedisClient client = new RedisClient(stringRedisTemplate);
				long currentTime = System.currentTimeMillis();
				for (String time : interval) {
					long mId = idUtil.nextId();
					long delayTime = currentTime + Long.valueOf(time) * 1000;
					String redisMsg = payLoad + "@@" + delayTime;
					client.addKeyString(mId + "", redisMsg);

					String bucketKey = getBucketKey(mId);
					String value = mId + "@@" + delayTime;
					client.addZset(bucketKey, value, delayTime);
					logger.info("存入Redis------mId: {}, {}+{}={}, message: {}, bucketKey: {}", mId, currentTime, time, delayTime, payLoad, bucketKey);
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private String getBucketKey(long id) {
		return "CYSBUCKET_" + (id % threadSize);
	}

}
