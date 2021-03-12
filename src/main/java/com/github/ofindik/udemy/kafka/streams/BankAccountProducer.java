package com.github.ofindik.udemy.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ofindik.udemy.kafka.streams.pojo.BankRequest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class BankAccountProducer {

	public static void main (String[] args) throws InterruptedException, JsonProcessingException, ExecutionException {
		Properties properties = new Properties ();
		properties.put (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put (ProducerConfig.ACKS_CONFIG, "all");
		properties.put (ProducerConfig.RETRIES_CONFIG, "3");
		properties.put (ProducerConfig.LINGER_MS_CONFIG, "1");
		properties.put (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		Producer<String, String> producer = new KafkaProducer<> (properties);

		for (int i = 0; i < 10; i++) {
			String name = getNewName ();
			producer.send (
				new ProducerRecord<String, String> ("bank-account-input", name, getNewBankRequest (name))).get ();
			Thread.sleep (10);
		}

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime ().addShutdownHook (new Thread (producer::close));
	}

	private static String getNewName () throws JsonProcessingException {
		String[] nameList = {"Osman", "Ali", "Hasan", "Hüsayin", "Mehmed"};
		Random random = new Random ();
		return nameList[random.nextInt (nameList.length)];
	}

	private static String getNewBankRequest (String name) throws JsonProcessingException {
		Random random = new Random ();
		BankRequest bankRequest = new BankRequest ();
		bankRequest.setName (name);
		bankRequest.setAmount (random.nextInt (100));
		bankRequest.setTime (Instant.now ().toString ());
		return new ObjectMapper ().writeValueAsString (bankRequest);
	}
}
