package com.example.demo.producer;

import java.io.Serializable;
import java.util.Properties;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

@Component
public class KafkaProducer implements InitializingBean{
	
	@Value("${kafka.producer.topic}")
	private String kafkaTopic;
	
	@Value("${kafka.producer.broker-list}")
	private String brokerList;
	
	@Value("${kafka.producer.client-id}")
	private String clientId;
	
	@Value("${kafka.producer.required-acks}")
	private String requiredAcks;
	
	private Producer<String, String> producer;
	
	public void send(String key, Serializable syncData){ // 发送实体
		KeyedMessage<String, String> message = new KeyedMessage<String, String>(kafkaTopic, key, JSON.toJSONString(syncData));
		producer.send(message);
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("client.id", clientId);
		props.put("request.required.acks", requiredAcks);
		props.put("serializer.class", "kafka.serializer.StringEncoder"); // value的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder"); // key的序列化类
		producer = new Producer<String, String>(new ProducerConfig(props));
		System.out.println("producer配置初始化结束");
	}

}
