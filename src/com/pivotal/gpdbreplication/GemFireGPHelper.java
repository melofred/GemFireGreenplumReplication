package com.pivotal.gpdbreplication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.Query;

public class GemFireGPHelper {
	
	private static GemFireGPHelper instance;
	
	GemFireGPConfigurator configurator;
	
	static ClientCacheFactory site1Factory;
	static ClientCache site1;
	
	static ClientCacheFactory site2Factory;
	static ClientCache site2;
	
	final static String GP_TABLES_OFFSET_REGION_NAME="GPTablesRowOffset";
	
	
	static{
		try{
			Class.forName("org.postgresql.Driver");
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	
	public static synchronized GemFireGPHelper getInstance() {
		if (instance==null) {
			instance = new GemFireGPHelper();
			instance.configure();
		}
		return instance;
		
	}
	
	
	private void configure() {
		if (configurator==null) {
			configurator = GemFireGPConfigurator.getInstance();
		}
	}
	

	
	public void resetDefaultLimitAndOffset(String table) {

		ClientCache cache = getGemFireSite1Connection();
		Region<String,Long> tablesOffsetRegion = cache.getRegion(GP_TABLES_OFFSET_REGION_NAME);
		tablesOffsetRegion.put(table, 0l);
		
	}	
	public Long getOffset(String table) {
		
		ClientCache cache = getGemFireSite1Connection();
		Region<String,Long> tablesOffsetRegion = cache.getRegion(GP_TABLES_OFFSET_REGION_NAME);
		 if (!tablesOffsetRegion.containsKeyOnServer(table)) {
		 
			tablesOffsetRegion.put(table, 0l);
		}
		return tablesOffsetRegion.get(table);
	}
	
	public int getBatchSize(String table) {
		
		return configurator.gpAOTablesBatchSize;
	}
		
	public Connection getGPConnection() throws SQLException{
		
		return DriverManager.getConnection(configurator.gpSite1URL, configurator.gpSite1Username, configurator.gpSite1Password);
		
	}	

	public Connection getGPConnectionSite2() throws SQLException {
		
		return DriverManager.getConnection(configurator.gpSite2URL, configurator.gpSite2Username, configurator.gpSite2Password);
		
	}
	
	public static ClientCache getLocalGFConnection() {
		return ClientCacheFactory.getAnyInstance();
	}

	public synchronized ClientCache getGemFireSite1Connection() {
		if (site1Factory==null) {
			Properties props = new Properties();
			props.setProperty("cache-xml-file", "client1.xml");
			site1Factory = new ClientCacheFactory(props);
//			site1Factory.setPoolPRSingleHopEnabled(true);
//			site1Factory.setPoolMaxConnections(50);
//			site1Factory.addPoolLocator(configurator.gemfireSite1Locator, configurator.gemfireSite1LocatorPort);
		}
		if (site1==null || site1.isClosed()) {
			site1 = site1Factory.create();
		}
		return site1;
	}

	public void updateOffset(String table, long newOffset) throws Exception {
		
		ClientCache cache = getGemFireSite1Connection();
		Region<String, Long> tablesOffset = cache.getRegion(GP_TABLES_OFFSET_REGION_NAME);
		tablesOffset.put(table, newOffset);
				
	}



	public synchronized long getNextTransactionLogId() {
		
		long nextId=1l;
		ClientCache cache = getGemFireSite1Connection();
		Region<String,Long> internalTransactions = cache.getRegion("internal_transactions");
		Long last = internalTransactions.get("last_log");
		if (last!=null) {
			nextId = last +1;
		}
		internalTransactions.put("last_log", nextId);
		return nextId;
		
		
	}
	
}
