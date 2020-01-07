package com.example.demo.controller;

import java.io.IOException;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.producer.KafkaProducer;
import com.example.demo.util.ApplicationContextUtil;

@RestController
public class ProduceController {
	
	private final static Logger logger = LoggerFactory.getLogger(ProduceController.class);

	@RequestMapping("/hello")
	public String hello() {
		return "Hello World Spring Boot Web";
	}

	@RequestMapping("/produce")
	public String produce(HttpServletRequest request) {
		try {
			String message = parseToString(request);
			String key = "log" + UUID.randomUUID().toString().replace("-", ""); // key
			logger.info("kafka生产消息, key: " + key + " message: " + message);
			KafkaProducer kafkaProducer = ApplicationContextUtil.getBean(KafkaProducer.class);
			kafkaProducer.send(key, message);
			return "success";
		} catch (IOException e) {
			e.printStackTrace();
			return "fail";
		}
	}

	private String parseToString(HttpServletRequest request) throws IOException {
		int contentLength = request.getContentLength();
		if (contentLength < 0) {
			return null;
		}
		byte buffer[] = new byte[contentLength];
		for (int i = 0; i < contentLength;) {

			int readlen = request.getInputStream().read(buffer, i, contentLength - i);
			if (readlen == -1) {
				break;
			}
			i += readlen;
		}
		String charEncoding = request.getCharacterEncoding();
		if (charEncoding == null) {
			charEncoding = "UTF-8";
		}
		return new String(buffer, charEncoding);
	}

}
