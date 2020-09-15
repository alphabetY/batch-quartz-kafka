package com.org.me.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonEOFException;
import com.org.me.model.User;

import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class UserJsonDeserializer extends JsonDeserializer<User> {

	@Override
	public User deserialize(String topic, byte[] data) {
		try {
			return super.deserialize(topic, data);
		} catch (Exception e) {
			System.out.println("Problem deserializing data " + new String(data) + " on topic " + topic);
			return null;
		}
	}

}
