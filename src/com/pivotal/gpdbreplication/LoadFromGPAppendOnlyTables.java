package com.pivotal.gpdbreplication;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;


public class LoadFromGPAppendOnlyTables {

	GemFireGPHelper helper;
	
	GPMetadataInfo metadata;
	
	
	static class WriteToExternalTablesThread extends Thread{
		
		private String table;
		GemFireGPHelper helper;
		

		public WriteToExternalTablesThread(String table, GemFireGPHelper helper) {
			super();
			this.table = table;
			this.helper = helper;
		}
		
		@Override
		public void run() {
			
			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO ");
			sql.append(table).append("_out ");
			sql.append("SELECT * from ").append(table);
			
			Connection conn = null;
			try {
				int limit = helper.getBatchSize(table);
				long offset = helper.getOffset(table);
				sql.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
				
				conn = helper.getGPConnection();
				int affectedRows = conn.createStatement().executeUpdate(sql.toString());
				
				helper.updateOffset(table, offset+affectedRows);
				
				conn.commit();
			}
			catch(Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			finally {
				if (conn!=null) {
					try {
						conn.close();
					} catch (SQLException e) {
						//ignore
					}
				}
			}
						
		}
		
	}
	
	public LoadFromGPAppendOnlyTables() {
		helper = GemFireGPHelper.getInstance();
		metadata = GPMetadataInfo.getInstance();
		
	}
	
	
	public void loadFromExternalTable() {

		Connection conn = null;
		try{
			// find out which tables to load

			String[] aoTables = metadata.getGPAppendOnlyTables();
			
			conn = helper.getGPConnection();
			
			for (String table: aoTables) {
				
				// for each append-only table, create an external table if doesn't exist already
				DatabaseMetaData mData = conn.getMetaData();
				ResultSet rs = mData.getTables(null, null, table+"_out", null);
				if (!rs.next()) {
				
					StringBuilder sql = new StringBuilder("CREATE WRITABLE EXTERNAL WEB TABLE ");
					sql.append(table).append("_out (LIKE ").append(table).append(") ");
					sql.append("EXECUTE '").append(helper.configurator.gpfDistLoadScriptPath).append(" ").append(table);
					sql.append("' FORMAT 'TEXT' (DELIMITER '|');");
					
					System.out.println("Creating External table for "+table);
					Statement stmt = conn.createStatement();
					stmt.executeUpdate(sql.toString());
					
				}

				
				// Call a Distribution Function on GemFire site 2 which will create the pipes, start a GPFDist instance and create a readable external table 
				// One GemFire must run at each segment server.
				/*
				ClientCache cache = GemFireGPHelper.getInstance().getGemFireSite2Connection();
				try {
					Execution execution = FunctionService.onRegion(cache.getRegion("GPFDistPorts"));
					execution.withArgs(table).execute("SetupGPBackupSiteFunction");
				}
				finally {
					cache.close();
				}
				*/
				helper.getGemFireSite1Connection().
					getRegion("setup_commands").put(System.currentTimeMillis(), table);
				
				
				// startLoad
				Thread writeToExternalTable = new WriteToExternalTablesThread(table,helper);
				writeToExternalTable.start();
				
				
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally {
			if (conn!=null) {
				try {
					conn.close();
				} catch (SQLException e) {
					//ignore
				}
			}
		}
					
			
		
	}
	

	
}
