package com.github.ofindik.udemy.kafka.streams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomJsonDeserializer<T> implements Deserializer<T> {
	private final ObjectMapper objectMapper = new ObjectMapper ();

	private Class<T> tClass;

	CustomJsonDeserializer () {
	}

	CustomJsonDeserializer (Class<T> tClass) {
		this.tClass = tClass;
	}

	@Override
	public T deserialize (String s, byte[] bytes) {
		if (bytes == null) {
			return null;
		} else {
			try {
				return objectMapper.readValue (bytes, tClass);
			} catch (Exception exception) {
				throw new SerializationException (exception);
			}
		}
	}
}
