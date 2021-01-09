package com.kafka.sample.demo1;

import com.kafka.sample.demo1.common.Foo2;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@SpringBootApplication
public class Demo1Application {

	private final Logger log = LoggerFactory.getLogger(Demo1Application.class);

	private final TaskExecutor exec = new SimpleAsyncTaskExecutor();

	public static void main(String[] args) {
		SpringApplication.run(Demo1Application.class, args);
	}

	/*
	 * Boot will autowire this into the container factory.
	 */
	@Bean
	public SeekToCurrentErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
		return new SeekToCurrentErrorHandler(
				new DeadLetterPublishingRecoverer((KafkaOperations)template), new FixedBackOff(1000L, 2));
	}

	/**
	 * Critical to convert String JSON to POJO
	 * @return
	 */
	@Bean
	public RecordMessageConverter converter() {
		return new StringJsonMessageConverter();
	}

	@KafkaListener(id="fooGroup", topics="topic1")
	public void listen(Foo2 foo){

		log.info("Recieved: " + foo);
		if (foo.getFoo().startsWith("fail")) {

			throw new RuntimeException("failed");
		}

		exec.execute(()-> System.out.println("Hit Enter to terminate..."));
	}

	@KafkaListener(id = "dltGroup", topics = "topic1.DLT")
	public void dltListen(String in) {
		log.info("Received from DLT: " + in);
		this.exec.execute(() -> System.out.println("Hit Enter to terminate..."));
	}

	/**
	 * Create topic
	 * @return
	 */
	@Bean
	public NewTopic topic() {
		return new NewTopic("topic1", 1, (short) 1);
	}

	/**
	 * Create topic
	 * @return
	 */
	@Bean
	public NewTopic dlt() {
		return new NewTopic("topic1.DLT", 1, (short) 1);
	}

}
