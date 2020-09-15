package com.org.me.kafka.consumer;

import org.apache.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

import com.org.me.model.User;

@Service
public class KafkaConsumerService {

	private static final Logger logger = Logger.getLogger(KafkaConsumerService.class);


	@KafkaListener(topics= "${kafka.csv.topic}", containerFactory = "kafkaListenerContainerFactory", groupId = ("${kafka.csv.group.id}"))
	//@KafkaListener(topics= "${kafka.csv.topic}", containerFactory = "kafkaListenerContainerFactory",groupId = "")
	public void consume(User user){
		logger.info("Consuming new message: " + user.getName() + " -> " + user);
	}


}
