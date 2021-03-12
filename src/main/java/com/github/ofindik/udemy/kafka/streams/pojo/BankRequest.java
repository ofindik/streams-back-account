package com.github.ofindik.udemy.kafka.streams.pojo;

public class BankRequest {
	private String name;
	private Integer amount;
	private String time;

	public String getName () {
		return name;
	}

	public void setName (String name) {
		this.name = name;
	}

	public Integer getAmount () {
		return amount;
	}

	public void setAmount (Integer amount) {
		this.amount = amount;
	}

	public String getTime () {
		return time;
	}

	public void setTime (String time) {
		this.time = time;
	}

	@Override
	public String toString () {
		return "BankRequest{" +
			"name='" + name + '\'' +
			", amount=" + amount +
			", time=" + time +
			'}';
	}
}
