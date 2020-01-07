package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.demo.consumer.ConsumerListener;
import com.example.demo.producer.ProducerListener;

@SpringBootApplication
public class RedisKafkaDemoApplication {

	public static void main(String[] args) {
		SpringApplication springApplication = new SpringApplication(RedisKafkaDemoApplication.class);
		springApplication.addListeners(new ConsumerListener());
		springApplication.addListeners(new ProducerListener());
		springApplication.run(args);
	}
	
}
