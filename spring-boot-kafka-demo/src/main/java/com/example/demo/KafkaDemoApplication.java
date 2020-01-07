package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.demo.consumer.ConsumerListener;

@SpringBootApplication
public class KafkaDemoApplication {

	public static void main(String[] args) {
		SpringApplication springApplication = new SpringApplication(KafkaDemoApplication.class);
		springApplication.addListeners(new ConsumerListener());
		springApplication.run(args);
	}
	
}
