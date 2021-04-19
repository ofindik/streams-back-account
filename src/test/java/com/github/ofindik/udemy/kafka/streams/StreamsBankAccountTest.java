package com.github.ofindik.udemy.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ofindik.udemy.kafka.streams.pojo.BankRequest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class StreamsBankAccountTest {

	TopologyTestDriver testDriver;

	@Before
	public void setUpTopologyTestDriver () {
		Properties config = new Properties ();
		config.put (StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		config.put (StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());
		config.put (StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());

		StreamsBankAccount streamsBankAccount = new StreamsBankAccount ();
		Topology topology = streamsBankAccount.createTopology ();
		testDriver = new TopologyTestDriver (topology, config);
	}

	@After
	public void closeTestDriver () {
		testDriver.close ();
	}

	@Test
	public void makeSureInitialTransactionIsCorrect () {
		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic ("bank-account-input",
			new StringSerializer (), new StringSerializer ());
		TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic ("bank-account-output",
			new StringDeserializer (), new StringDeserializer ());

		inputTopic.pipeInput ("Osman", getNewBankRequest ("Osman", 10));
		inputTopic.pipeInput ("Hasan", getNewBankRequest ("Hasan", 100));

		assertThat ("Output topic is empty", outputTopic.isEmpty (), is (false));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Osman", "{\"balance\":10," +
			"\"transactionCount\":1}")));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Hasan", "{\"balance\":100," +
			"\"transactionCount\":1}")));
	}

	@Test
	public void makeSureTransactionsForSameAccountAreCorrect () {
		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic ("bank-account-input",
			new StringSerializer (), new StringSerializer ());
		TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic ("bank-account-output",
			new StringDeserializer (), new StringDeserializer ());

		inputTopic.pipeInput ("Osman", getNewBankRequest ("Osman", 10));
		inputTopic.pipeInput ("Osman", getNewBankRequest ("Osman", 100));

		assertThat ("Output topic is empty", outputTopic.isEmpty (), is (false));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Osman", "{\"balance\":10," +
			"\"transactionCount\":1}")));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Osman", "{\"balance\":110," +
			"\"transactionCount\":2}")));
	}

	@Test
	public void makeSureTransactionsForDifferentAccountAreCorrect () {
		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic ("bank-account-input",
			new StringSerializer (), new StringSerializer ());
		TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic ("bank-account-output",
			new StringDeserializer (), new StringDeserializer ());

		inputTopic.pipeInput ("Osman", getNewBankRequest ("Osman", 30));
		inputTopic.pipeInput ("Hasan", getNewBankRequest ("Hasan", 70));
		inputTopic.pipeInput ("Hasan", getNewBankRequest ("Hasan", 50));
		inputTopic.pipeInput ("Osman", getNewBankRequest ("Osman", 220));

		assertThat ("Output topic is empty", outputTopic.isEmpty (), is (false));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Osman", "{\"balance\":30," +
			"\"transactionCount\":1}")));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Hasan", "{\"balance\":70," +
			"\"transactionCount\":1}")));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Hasan", "{\"balance\":120," +
			"\"transactionCount\":2}")));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("Osman", "{\"balance\":250," +
			"\"transactionCount\":2}")));
	}

	private String getNewBankRequest (String name, int amount) {
		String result = null;
		try {
			BankRequest bankRequest = new BankRequest ();
			bankRequest.setName (name);
			bankRequest.setAmount (amount);
			bankRequest.setTime (Instant.now ().toString ());
			result = new ObjectMapper ().writeValueAsString (bankRequest);
		} catch (JsonProcessingException e) {
			e.printStackTrace ();
		}
		return result;
	}
}
