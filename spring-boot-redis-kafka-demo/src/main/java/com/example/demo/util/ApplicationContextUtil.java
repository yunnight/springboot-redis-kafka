package com.example.demo.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ApplicationContextUtil implements ApplicationContextAware {

	private static ApplicationContext ctx;

	@Override
	public void setApplicationContext(ApplicationContext arg0) throws BeansException {
		ctx = arg0;
	}

	public ApplicationContext getApplicationContext() {
		return ctx;
	}

	public static <T> T getBean(Class<T> type) {
		return (T) ctx.getBean(type);
	}

}
