package com.pivotal.gpdbreplication.gplog;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.query.Query;
import com.pivotal.gpdbreplication.GPMetadataInfo;
import com.pivotal.gpdbreplication.out.ConfiguredAsyncEventListener;

public class LogFileEventsAsyncListener extends ConfiguredAsyncEventListener {



	@Override
	public boolean processEvents(List<AsyncEvent> events) {
	
		try {
			// wait for some time for late errors to arrive
			Thread.sleep(2000);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		String[] aoTables = null;
		try {
			aoTables = GPMetadataInfo.getInstance().getGPAppendOnlyTables();
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1);
		}
		
		for (AsyncEvent e:events) {
			try {
				long internalId = (Long)e.getKey();
				Statement stmt = (Statement)e.getDeserializedValue();
				
				String transactionId = stmt.getTransactionId();
				String statement = stmt.getStatement();
				
				
				// check if the event was not reported afterwards as error
				Query query = e.getRegion().getRegionService().getQueryService().newQuery("select id from /error_statements where transactionId=$1");
				Object result = query.execute(new Object[] {transactionId});
				boolean isError = (result!=null && !((Collection)result).isEmpty());
				
				
				// check if it was not an INSERT into an append_only table (those will be taken care by a different process)
				boolean isAOInsert = false;
				if (!isError) {
					if (statement.toLowerCase().trim().startsWith("\"insert into")){
						int startIndexOfTableName = statement.toLowerCase().indexOf("\"insert into ")+"\"insert into ".length();
						String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(" ", startIndexOfTableName));
						for (String aoTable:aoTables) {
							if (tableName.equalsIgnoreCase(aoTable)) isAOInsert=true;
						}
					}
				}

				/* Append-Only Tables cases */
				
				// truncate an append-only table
				if (!isError && statement.toLowerCase().trim().startsWith("\"truncate ")) {
					int startIndexOfTableName = statement.toLowerCase().indexOf("\"truncate ")+"\"truncate ".length();
					String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
					for (String aoTable:aoTables) {
						if (tableName.equalsIgnoreCase(aoTable)) {
							helper.resetDefaultLimitAndOffset(aoTable);
						}
					}					
				}

				// drop an append-only table
				if (!isError && statement.toLowerCase().trim().startsWith("\"drop table ")) {
					int startIndexOfTableName = statement.toLowerCase().indexOf("\"drop table ")+"\"drop table ".length();
					String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
					for (String aoTable:aoTables) {
						if (tableName.equalsIgnoreCase(aoTable)) {
							helper.resetDefaultLimitAndOffset(aoTable);
						}
					}					
				}
				
				if (!isError && !isAOInsert) {
					
					e.getRegion().getRegionService().getRegion("ready_statements").put(internalId, stmt);
					
				}
				
				
				
				
			}
			catch(Exception ex) {
				ex.printStackTrace();
				throw new RuntimeException(ex);
			}
		}
		
		return true;
	}



}
