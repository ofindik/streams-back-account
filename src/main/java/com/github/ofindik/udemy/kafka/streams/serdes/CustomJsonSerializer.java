package com.github.ofindik.udemy.kafka.streams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class CustomJsonSerializer<T> implements Serializer<T> {
	private final ObjectMapper objectMapper = new ObjectMapper ();

	@Override
	public byte[] serialize (String s, T data) {
		if (data == null) {
			return null;
		} else {
			try {
				return this.objectMapper.writeValueAsBytes (data);
			} catch (Exception exception) {
				throw new SerializationException ("Error serializing JSON message", exception);
			}
		}
	}
}
