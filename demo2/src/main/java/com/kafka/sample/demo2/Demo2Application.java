package com.kafka.sample.demo2;

import com.kafka.sample.demo2.common.Bar2;
import com.kafka.sample.demo2.common.Foo2;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class Demo2Application {

	public static void main(String[] args) {
		SpringApplication.run(Demo2Application.class, args);
	}

	/*
	 * Boot will autowire this into the container factory.
	 */
	@Bean
	public SeekToCurrentErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
		return new SeekToCurrentErrorHandler(
				new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 2));
	}

	@Bean
	public RecordMessageConverter converter() {
		StringJsonMessageConverter converter = new StringJsonMessageConverter();
		DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
		typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
		typeMapper.addTrustedPackages("com.kafka.sample.demo2.common");
		Map<String, Class<?>> mappings = new HashMap<>();
		mappings.put("foo", Foo2.class);
		mappings.put("bar", Bar2.class);
		typeMapper.setIdClassMapping(mappings);
		converter.setTypeMapper(typeMapper);
		return converter;
	}

	@Bean
	public NewTopic foos() {
		return new NewTopic("foos", 1, (short) 1);
	}

	@Bean
	public NewTopic bars() {
		return new NewTopic("bars", 1, (short) 1);
	}
}
