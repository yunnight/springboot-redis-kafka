package com.example.demo.controller;

import java.util.Random;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestRedisController {
	
	@Autowired
	private StringRedisTemplate stringRedisTemplate;

	@GetMapping("/set/add/{val}")
	public String testSet(@PathVariable("val") String value){
		SetOperations<String, String> opsForSet = stringRedisTemplate.opsForSet();
		opsForSet.add("cysSet", value);
		Set<String> members = opsForSet.members("cysSet");
		return members.toString();
	}
	
	@GetMapping("/zset/add/{val}")
	public String testZset(@PathVariable("val") String value){
		ZSetOperations<String, String> opsForZSet = stringRedisTemplate.opsForZSet();
		double score = 0d;
		Random random = new Random();
		score = random.nextDouble();
		
		opsForZSet.add("cysZset", value, score);
		Set<TypedTuple<String>> rangeWithScores = opsForZSet.rangeWithScores("cysZset", 0l, 100l);
		System.out.println("-------------------------------------------------------------");
		for (TypedTuple<String> tuple: rangeWithScores){
			System.out.println("value:"+tuple.getValue()+" score:"+tuple.getScore());
		}
		System.out.println("-------------------------------------------------------------");
		
		Set<String> range = opsForZSet.range("cysZset", 0l, 0l); // 取第一个元素（分数最小）
		return range.toString();
	}
	
}
