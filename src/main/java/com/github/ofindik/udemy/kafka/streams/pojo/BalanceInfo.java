package com.github.ofindik.udemy.kafka.streams.pojo;

public class BalanceInfo {
	private Integer balance;
	private Integer transactionCount;

	public Integer getBalance () {
		return balance;
	}

	public void setBalance (Integer balance) {
		this.balance = balance;
	}

	public Integer getTransactionCount () {
		return transactionCount;
	}

	public void setTransactionCount (Integer transactionCount) {
		this.transactionCount = transactionCount;
	}

	@Override
	public String toString () {
		return "BalanceInfo{" +
			"balance=" + balance +
			", transactionCount=" + transactionCount +
			'}';
	}
}
