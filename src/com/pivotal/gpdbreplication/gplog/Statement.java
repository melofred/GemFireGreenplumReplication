package com.pivotal.gpdbreplication.gplog;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

public class Statement implements PdxSerializable {

	private String statement;
	private String transactionId;
	private String databaseName;
	private String timestamp;
	
	public String getStatement() {
		return statement;
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public void fromData(PdxReader reader) {
		statement = reader.readString("statement");
		transactionId = reader.readString("transactionId");
		databaseName = reader.readString("databaseName");
		timestamp = reader.readString("timestamp");

	}

	@Override
	public void toData(PdxWriter writer) {
		writer.writeString("statement", statement);
		writer.writeString("transactionId", transactionId);
		writer.writeString("databaseName", databaseName);
		writer.writeString("timestamp", timestamp);
	}

}
