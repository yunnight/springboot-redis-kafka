package com.example.demo.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

@Component
public class KafkaConsumer implements InitializingBean{
	private final static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
	
	@Value("${kafka.consumer.topic}")
	private String kafkaTopic;
	
	@Value("${kafka.consumer.zookeeper.session-timeout}")
	private String zkSessionTimeout;
	
	@Value("${kafka.consumer.zookeeper.auto-offset.reset}")
	private String offsetReset;
	
	@Value("${kafka.consumer.zookeeper.connection}")
	private String zkConnection;
	
	@Value("${kafka.consumer.zookeeper.auto-commit.enable}")
	private boolean autoCommitEnable;
	
	@Value("${kafka.consumer.zookeeper.auto-commit.interval-ms}")
	private String intervalMs;
	
	@Value("${kafka.consumer.thread-size}")
	private Integer threadSize;
	
	@Value("${kafka.consumer.group-id}")
	private String groupId;
	
	public ConsumerConnector consumer;
	
	private List<KafkaStream<byte[], byte[]>> kafkaStreamList;
	
	public List<KafkaStream<byte[], byte[]>> getKafkaStreamList() {
		return kafkaStreamList;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Properties props = new Properties();
		props.put("zookeeper.connect", zkConnection);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", zkSessionTimeout);
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", intervalMs);
		props.put("auto.offset.rset", offsetReset);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(kafkaTopic, threadSize);
		Map<String, List<KafkaStream<byte[], byte[]>>> allTopics = consumer.createMessageStreams(topicMap);
		kafkaStreamList = allTopics.get(kafkaTopic);
		
		logger.info("consumer配置初始化结束");
		
	}

}
