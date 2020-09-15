package com.org.me.batch;

import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.org.me.kafka.producer.KafkaProducerService;
import com.org.me.model.User;

import java.util.List;

@Component
public class UserDbWriter implements ItemWriter<User> {

	private static final Logger logger = Logger.getLogger(UserDbWriter.class);

	@Autowired
	private KafkaProducerService kafkaProducerService;

	/**
	 * ItemWriter should send to Kafka for stream processing
	 * @param users
	 * @throws Exception
	 */
	@Override
	public void write(List<? extends User> users) throws Exception {
		logger.info("Saving user list: " + users);
		kafkaProducerService.publishUser(users);
	}
}
