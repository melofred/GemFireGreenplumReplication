package com.pivotal.gpdbreplication.gplog;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.query.Query;
import com.pivotal.gpdbreplication.GPMetadataInfo;
import com.pivotal.gpdbreplication.GemFireGPHelper;
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
				
				boolean isAODDL = false;
				// alter table
				if (!isError && statement.toLowerCase().trim().startsWith("\"alter table")) {
					int startIndexOfTableName = statement.toLowerCase().indexOf("\"alter table  ")+"\"alter table ".length();
					String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
					for (String aoTable:aoTables) {
						if (tableName.equalsIgnoreCase(aoTable)) {
							isAODDL = true;
						}
					}					
					
				}
				
				// truncate an append-only table
				else if (!isError && statement.toLowerCase().trim().startsWith("\"truncate table")) {
					int startIndexOfTableName = statement.toLowerCase().indexOf("\"truncate table ")+"\"truncate table ".length();
					String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
					
					for (String aoTable:aoTables) {
						if (tableName.equalsIgnoreCase(aoTable)) {
							resetDefaultLimitAndOffset(e.getRegion().getRegionService(),aoTable);
							isAODDL = true;
						}
					}					
				}
				else if (!isError && statement.toLowerCase().trim().startsWith("\"truncate")) {
					int startIndexOfTableName = statement.toLowerCase().indexOf("\"truncate ")+"\"truncate ".length();
					String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
					for (String aoTable:aoTables) {
						if (tableName.equalsIgnoreCase(aoTable)) {
							resetDefaultLimitAndOffset(e.getRegion().getRegionService(),aoTable);
							isAODDL = true;
						}
					}					
				}


				// drop an append-only table
				else if (!isError && statement.toLowerCase().trim().startsWith("\"drop table")) {
					int startIndexOfTableName = statement.toLowerCase().indexOf("\"drop table ")+"\"drop table ".length();
					String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
					for (String aoTable:aoTables) {
						if (tableName.equalsIgnoreCase(aoTable)) {
							resetDefaultLimitAndOffset(e.getRegion().getRegionService(),aoTable);
							isAODDL = true;
						}
					}					
				}
				
				// create an append-only table
				else if (!isError && statement.toLowerCase().trim().startsWith("\"create table")) {					
					if (statement.toLowerCase().indexOf("appendonly=true")!=-1) {
						int startIndexOfTableName = statement.toLowerCase().indexOf("\"create table ")+"\"create table ".length();
						String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
						resetDefaultLimitAndOffset(e.getRegion().getRegionService(),tableName);
						isAODDL=true;
					}
				}
				
				if(!isError && isAODDL){
					StringBuilder aoStatementKey = new StringBuilder("_AO_DDL_");
					aoStatementKey.append(UUID.randomUUID().toString());
					e.getRegion().getRegionService().getRegion("AO_rows").put(aoStatementKey.toString(), statement);					
					
				}
				else if (!isError && !isAOInsert) {
					
					e.getRegion().getRegionService().getRegion("ready_statements").put(internalId, statement);
					
				}
				
				
				
				
			}
			catch(Exception ex) {
				ex.printStackTrace();
				throw new RuntimeException(ex);
			}
		}
		
		return true;
	}

	public void resetDefaultLimitAndOffset(RegionService svc, String table) {

		Region<String,Long> tablesOffsetRegion = svc.getRegion(GemFireGPHelper.GP_TABLES_OFFSET_REGION_NAME);
		tablesOffsetRegion.put(table, 0l);
		
	}	


}
