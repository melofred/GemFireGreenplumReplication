package com.pivotal.gpdbreplication;

import java.util.UUID;


public class GpGFTest {

	public static void main(String[] args) {
		
		String statement="\"truncate table my_ao_table;\"";
		
		String[] aoTables = new String[]{"my_ao_table","my_ao_table2"};
		
		boolean isError=false;
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
			
			System.out.println("TRYING TO TRUNCATE TABLE "+tableName);
			for (String aoTable:aoTables) {
				if (tableName.equalsIgnoreCase(aoTable)) {
					//resetDefaultLimitAndOffset(e.getRegion().getRegionService(),aoTable);
					isAODDL = true;
				}
			}					
		}
		else if (!isError && statement.toLowerCase().trim().startsWith("\"truncate")) {
			int startIndexOfTableName = statement.toLowerCase().indexOf("\"truncate ")+"\"truncate ".length();
			String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
			for (String aoTable:aoTables) {
				if (tableName.equalsIgnoreCase(aoTable)) {
					//resetDefaultLimitAndOffset(e.getRegion().getRegionService(),aoTable);
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
					//resetDefaultLimitAndOffset(e.getRegion().getRegionService(),aoTable);
					isAODDL = true;
				}
			}					
		}
		
		// create an append-only table
		else if (!isError && statement.toLowerCase().trim().startsWith("\"create table")) {					
			if (statement.toLowerCase().indexOf("appendonly=true")!=-1) {
				int startIndexOfTableName = statement.toLowerCase().indexOf("\"create table ")+"\"create table ".length();
				String tableName = statement.toLowerCase().substring(startIndexOfTableName,statement.indexOf(";", startIndexOfTableName)).trim();
				//resetDefaultLimitAndOffset(e.getRegion().getRegionService(),tableName);
				isAODDL=true;
			}
		}
		
		if(!isError && isAODDL){
			StringBuilder aoStatementKey = new StringBuilder("_AO_DDL_");
			aoStatementKey.append(UUID.randomUUID().toString());
			//e.getRegion().getRegionService().getRegion("AO_rows").put(aoStatementKey, statement);					
			
		}
		else if (!isError && !isAOInsert) {
			
			//e.getRegion().getRegionService().getRegion("ready_statements").put(internalId, statement);
			
		}
		
		
		
	}
	
}
