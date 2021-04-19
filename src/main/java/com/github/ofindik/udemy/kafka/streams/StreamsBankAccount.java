package com.github.ofindik.udemy.kafka.streams;

import com.github.ofindik.udemy.kafka.streams.pojo.BalanceInfo;
import com.github.ofindik.udemy.kafka.streams.pojo.BankRequest;
import com.github.ofindik.udemy.kafka.streams.serdes.CustomSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class StreamsBankAccount {
	public static void main (String[] args) {
		Properties config = new Properties ();
		config.put (StreamsConfig.APPLICATION_ID_CONFIG, "streams-bank-account");
		config.put (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
		config.put (StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		config.put (StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		StreamsBuilder streamsBuilder = new StreamsBuilder ();
		// 1 - Read one topic from Kafka (KStream)
		KStream<String, BankRequest> bankAccountStream = streamsBuilder.stream ("bank-account-input",
			Consumed.with (Serdes.String (), CustomSerdes.BankRequest ()));
		// 2 - gropuByKey
		bankAccountStream.groupByKey (Grouped.with (Serdes.String (), CustomSerdes.BankRequest ()))
			// 3 - aggregate
			.aggregate (() -> getInitialBalanceInfo (),
				(name, bankRequest, balanceInfo) -> getBalanceInfo (bankRequest, balanceInfo),
				Materialized.with (Serdes.String (), CustomSerdes.BalanceInfo ())
			)
			// 4 - Write to Kafka
			.toStream ().to ("bank-account-output", Produced.with (Serdes.String (), CustomSerdes.BalanceInfo ()));

		KafkaStreams kafkaStreams = new KafkaStreams (streamsBuilder.build (), config);
		kafkaStreams.start ();

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime ().addShutdownHook (new Thread (kafkaStreams::close));
	}

	private static BalanceInfo getInitialBalanceInfo () {
		BalanceInfo initialBalanceInfo = new BalanceInfo ();
		initialBalanceInfo.setBalance (0);
		initialBalanceInfo.setTransactionCount (0);
		return initialBalanceInfo;
	}

	private static BalanceInfo getBalanceInfo (Object bankRequest, Object balanceInfo) {
		BalanceInfo result = new BalanceInfo ();
		result.setBalance (((BankRequest) bankRequest).getAmount () + ((BalanceInfo) balanceInfo).getBalance ());
		result.setTransactionCount (((BalanceInfo) balanceInfo).getTransactionCount () + 1);
		return result;
	}
}
