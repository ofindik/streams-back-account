package com.github.ofindik.udemy.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class StreamsBackAccount {
	public static void main (String[] args) {
		Properties config = new Properties ();
		config.put (StreamsConfig.APPLICATION_ID_CONFIG, "streams-bank-account");
		config.put (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put (StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());
		config.put (StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());

		// we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
		config.put (StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		config.put (StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		StreamsBuilder streamsBuilder = new StreamsBuilder ();
		// 1 - Read one topic from Kafka (KStream)
		KStream<String, String> bankAccountStream = streamsBuilder.stream ("bank-account-input");
		// 2 - gropuByKey
		bankAccountStream.groupByKey (Grouped.with (Serdes.String (), Serdes.String ()))
			// 3 - aggregate
			.aggregate (() -> getInitialBalanceInfo (),
				(name, bankRequest, balanceInfo) -> getBalanceInfo (bankRequest, balanceInfo),
				Materialized.with (Serdes.String (), Serdes.String ())
			)
			// 4 - Write to Kafka
			.toStream ().to ("bank-account-output");

		KafkaStreams kafkaStreams = new KafkaStreams (streamsBuilder.build (), config);
		kafkaStreams.start ();

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime ().addShutdownHook (new Thread (kafkaStreams::close));
	}

	private static String getInitialBalanceInfo () {
		BalanceInfo initialBalanceInfo = new BalanceInfo ();
		initialBalanceInfo.setBalance (0);
		initialBalanceInfo.setTransactionCount (0);
		try {
			return new ObjectMapper ().writeValueAsString (initialBalanceInfo);
		} catch (JsonProcessingException e) {
			e.printStackTrace ();
			return null;
		}
	}

	private static String getBalanceInfo (String bankRequestString, String balanceInfoString) {
		ObjectMapper objectMapper = new ObjectMapper ();
		try {
			BankRequest bankRequest = objectMapper.readValue (bankRequestString, BankRequest.class);
			BalanceInfo balanceInfo = objectMapper.readValue (balanceInfoString, BalanceInfo.class);

			BalanceInfo result = new BalanceInfo ();
			result.setBalance (bankRequest.getAmount () + balanceInfo.getBalance ());
			result.setTransactionCount (balanceInfo.getTransactionCount () + 1);
			return objectMapper.writeValueAsString (result);
		} catch (JsonProcessingException e) {
			e.printStackTrace ();
			return null;
		}
	}
}
