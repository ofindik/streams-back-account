package com.github.ofindik.udemy.kafka.streams.serdes;

import com.github.ofindik.udemy.kafka.streams.pojo.BalanceInfo;
import com.github.ofindik.udemy.kafka.streams.pojo.BankRequest;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CustomSerdes {

	public static Serde<BankRequest> BankRequest () {
		return new CustomSerdes.BankRequestSerde ();
	}

	public static Serde<BalanceInfo> BalanceInfo () {
		return new CustomSerdes.BalanceInfoSerde ();
	}

	public static final class BankRequestSerde extends Serdes.WrapperSerde<BankRequest> {
		public BankRequestSerde () {
			super (new CustomJsonSerializer<> (), new CustomJsonDeserializer<> (BankRequest.class));
		}
	}

	public static final class BalanceInfoSerde extends Serdes.WrapperSerde<BalanceInfo> {
		public BalanceInfoSerde () {
			super (new CustomJsonSerializer<> (), new CustomJsonDeserializer<> (BalanceInfo.class));
		}
	}
}
