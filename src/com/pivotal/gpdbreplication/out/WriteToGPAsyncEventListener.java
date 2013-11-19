package com.pivotal.gpdbreplication.out;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.internal.cache.FilterProfile.interestType;
import com.pivotal.gpdbreplication.GPMetadataInfo;

public class WriteToGPAsyncEventListener extends ConfiguredAsyncEventListener {

	static String hostname;
	
	public WriteToGPAsyncEventListener() {
		super();
		// hostname
		try {
			Process process = Runtime.getRuntime().exec("hostname");
			hostname = new BufferedReader(new InputStreamReader(process.getInputStream())).readLine();
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean processEvents(List<AsyncEvent> events) {
		
		try {
			

			GPMetadataInfo info = GPMetadataInfo.getInstance();
			String[] tables = info.getGPAppendOnlyTables();
			Map<String,PrintWriter> pipesMap = new HashMap<String, PrintWriter>();
			
			// open the pipes and call the insert on GP 
			for (final String table:tables) {
			
				File file = new File(configurator.getGPFDistLoadPath(),table);
				if (!file.exists()) {
					System.out.println("COULD NOT OPEN PIPE FOR TABLE "+table);
				}

				else {

					// this thread will block until the pipes are flushed
					Thread t = new Thread() {
						@Override
						public void run() {

							StringBuilder sql = new StringBuilder();
							sql.append("INSERT INTO ").append(table).append(" ");
							sql.append("SELECT * FROM ").append(table).append("_in_").append(hostname).append(";");
							Connection conn = null;
							try {
								conn = helper.getGPConnectionSite2();
								int modifiedRows = conn.createStatement().executeUpdate(sql.toString());
								System.out.println("INSERTED "+modifiedRows+" ROWS");
								
							} catch (SQLException e) {
								e.printStackTrace();
							}					
							finally {
								if (conn!=null) try {conn.close();}catch(Exception e) {}
							}
						}
					};
					
					t.start();
					
					Thread.yield();
					
					pipesMap.put(table, new PrintWriter(new BufferedWriter(new FileWriter(file)),false));
					
					
				}
			}
			

			for (AsyncEvent event:events) {
				
				Object transactionId = event.getKey();
				
				String rowValue = (String)event.getDeserializedValue();
				StringBuffer buff = new StringBuffer(rowValue);
				
				int separator = buff.indexOf("|");
				String tableName = buff.substring(0, separator); 
				String line = buff.substring(separator+1);
				
				pipesMap.get(tableName).write(line);				
				pipesMap.get(tableName).println();			
				
			}
			
			for (final String table:tables) {
				pipesMap.get(table).flush();
				pipesMap.get(table).close();	
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch(IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe);
		}
		
		
		
		return true;
	}



}
