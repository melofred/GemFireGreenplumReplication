package com.pivotal.gpdbreplication;

import java.util.Properties;

public class GemFireGPConfigurator {
	
	private static GemFireGPConfigurator instance;
		
	int gpAOTablesBatchSize;

	String gpSite1URL;
	String gpSite1Username;
	String gpSite1Password;
	
	String gpSite2URL;	
	String gpSite2Username;	
	String gpSite2Password;	
	
	String gemfireSite1Locator;	
	int gemfireSite1LocatorPort;	
	
	String gemfireSite2Locator;	
	int gemfireSite2LocatorPort;	
	
	String gpfDistLoadScriptPath;
	
	String gpfdistLoadPath;
	
	String gpfDistErrorPath;

	int gpfDistInitialPort;

	public static synchronized GemFireGPConfigurator getInstance() {
		
		if (instance==null) {
			instance = new GemFireGPConfigurator();
			Properties props = new Properties();
			try {
				props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("replication.properties"));
				instance.gpAOTablesBatchSize = Integer.parseInt(props.getProperty("sqlfgp.greenplum.ao_tables.batch_size"));
				instance.gpfDistErrorPath = props.getProperty("sqlfgp.greenplum.gpfdist.error_path");
				instance.gpfDistInitialPort = Integer.parseInt(props.getProperty("sqlfgp.greenplum.gpfdist.initial_port"));
				instance.gpfdistLoadPath= props.getProperty("sqlfgp.greenplum.gpfdist.load_path");
				instance.gpSite1Password= props.getProperty("sqlfgp.greenplum.site1.password");
				instance.gpSite1URL= props.getProperty("sqlfgp.greenplum.site1.url");
				instance.gpSite1Username= props.getProperty("sqlfgp.greenplum.site1.username");
				instance.gpSite2Password= props.getProperty("sqlfgp.greenplum.site2.password");
				instance.gpSite2URL= props.getProperty("sqlfgp.greenplum.site2.url");
				instance.gpSite2Username= props.getProperty("sqlfgp.greenplum.site2.username");
				instance.gpfDistLoadScriptPath= props.getProperty("sqlfgp.load_script.path");
				instance.gemfireSite1Locator= props.getProperty("sqlfgp.gemfire.site1.locator.host");
				instance.gemfireSite1LocatorPort= Integer.parseInt(props.getProperty("sqlfgp.gemfire.site1.locator.port"));				
				instance.gemfireSite2Locator= props.getProperty("sqlfgp.gemfire.site2.locator.host");
				instance.gemfireSite2LocatorPort= Integer.parseInt(props.getProperty("sqlfgp.gemfire.site2.locator.port"));
				
				
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}			
		}
		
		
		return instance;
	}
	
	
	




	public String getLoadScriptPath() {
		return gpfDistLoadScriptPath;
	}
	
	public String getGPFDistLoadPath() {
		return gpfdistLoadPath;
	}

	public String getGPFDistErrorPath() {
		return gpfDistErrorPath; 
	}

	public int getGPFDistInitialPort() {
		return gpfDistInitialPort;
	}







	public int getGpAOTablesBatchSize() {
		return gpAOTablesBatchSize;
	}







	public String getGpSite1URL() {
		return gpSite1URL;
	}







	public String getGpSite1Username() {
		return gpSite1Username;
	}







	public String getGpSite1Password() {
		return gpSite1Password;
	}







	public String getGpSite2URL() {
		return gpSite2URL;
	}







	public String getGpSite2Username() {
		return gpSite2Username;
	}







	public String getGpSite2Password() {
		return gpSite2Password;
	}







	public String getGemfireSite1Locator() {
		return gemfireSite1Locator;
	}







	public int getGemfireSite1LocatorPort() {
		return gemfireSite1LocatorPort;
	}







	public String getGemfireSite2Locator() {
		return gemfireSite2Locator;
	}







	public int getGemfireSite2LocatorPort() {
		return gemfireSite2LocatorPort;
	}







	public String getGpfDistLoadScriptPath() {
		return gpfDistLoadScriptPath;
	}







	public String getGpfdistLoadPath() {
		return gpfdistLoadPath;
	}







	public String getGpfDistErrorPath() {
		return gpfDistErrorPath;
	}







	public int getGpfDistInitialPort() {
		return gpfDistInitialPort;
	}




	
}
