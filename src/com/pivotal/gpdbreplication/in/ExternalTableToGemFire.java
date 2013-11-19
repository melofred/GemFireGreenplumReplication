package com.pivotal.gpdbreplication.in;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.UUID;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.pivotal.gpdbreplication.GemFireGPHelper;

public class ExternalTableToGemFire {

	GemFireGPHelper helper;
	
	private String tableName;
	ClientCache cache;

	public ExternalTableToGemFire() {
		
	}
	
	public ExternalTableToGemFire(String tableName) {
		this.tableName = tableName;
	}
	
	private void startFlow() throws IOException{
		
				
		InputStream in = System.in;
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line = null;
		cache = helper.getGemFireSite1Connection();
		Region<String,String> appendOnlyRows = cache.getRegion("AO_rows");
		
		while ( (line=reader.readLine())!=null) {
			//long internalId = helper.getNextTransactionLogId();
			String uuid = UUID.randomUUID().toString();
			
			StringBuilder lineToInsert = new StringBuilder(tableName).append("|").append(line);
			appendOnlyRows.put(uuid, lineToInsert.toString());
		}
	}
	
	
	public static void main(String[] args) {

		String tableName = args[0];
		ExternalTableToGemFire flow = new ExternalTableToGemFire(tableName);
		flow.helper = GemFireGPHelper.getInstance();
		try{
			flow.startFlow();
		}
		catch(Exception e) {
			System.out.println("Error sending stream to SQLFire.");
			e.printStackTrace();
		}
		
	}
	
}