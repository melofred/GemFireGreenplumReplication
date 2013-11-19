package com.pivotal.gpdbreplication.out;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.BindException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.Query;
import com.pivotal.gpdbreplication.GemFireGPConfigurator;
import com.pivotal.gpdbreplication.GemFireGPHelper;

public class SetupGreenplumBackupSiteAOTables extends FunctionAdapter implements Declarable {
	
	static GemFireGPHelper helper;
	static GemFireGPConfigurator configurator;
	
	final static String GPFDIST_PORT_REGION_NAME="GPFDistPorts";

	static int GPFDIST_INITIAL_PORT;
	static String GPFDIST_LOAD_PATH;
	static String GPFDIST_ERROR_PATH;

	
	
	final static Map<String , Process>processMap = new Hashtable<String, Process>();
	
	
	public static int getGpfdistPort(String tablename) {
		
		try{
			Cache cache = CacheFactory.getAnyInstance();
			Region<String, Integer> ports = cache.getRegion(GPFDIST_PORT_REGION_NAME);
			if (!ports.containsKey(tablename)) {
				synchronized (ports) {
					int nextPort=0;
					StringBuilder queryStr = new StringBuilder("select distinct p from /").append(GPFDIST_PORT_REGION_NAME).append(" p order by p desc limit 1");
					Query query = ports.getRegionService().getQueryService().newQuery(queryStr.toString()); 
					Object result = query.execute();
					if (result==null || ((Collection)result).isEmpty()) {
						nextPort = configurator.getGPFDistInitialPort();
					}
					else {
						nextPort = Integer.parseInt( ((Collection)result).iterator().next().toString())+1;
					}
					System.out.println("CALLING PUT USING TABLENAME="+tablename+"  AND PORT="+nextPort);
					ports.put(tablename, nextPort);					
				}
			}
			return ports.get(tablename);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	
	public static void setup(final String tablename) {
		
		System.out.println("Setting up replication for table "+tablename);
		if (configurator==null) {
			configurator = GemFireGPConfigurator.getInstance();
			GPFDIST_INITIAL_PORT= configurator.getGPFDistInitialPort();
			GPFDIST_LOAD_PATH = configurator.getGPFDistLoadPath();
			GPFDIST_ERROR_PATH = configurator.getGPFDistErrorPath();
		}
		if (helper==null) {
			helper = GemFireGPHelper.getInstance();
		}

		
		Connection connection = null;
		try {
			
			File directory = new File(GPFDIST_LOAD_PATH);
			directory.mkdirs();
			File errDir = new File(GPFDIST_ERROR_PATH);
			errDir.mkdirs();
			
			File pipe = new File(tablename,directory.getAbsolutePath());
			if (!pipe.exists()) {
				//create the pipe
				Runtime.getRuntime().exec("mkfifo "+tablename, null, directory);				
			}
			
			final int GPFDIST_PORT = getGpfdistPort(tablename);
			
			StringBuilder gpfdistStr = new StringBuilder();
			gpfdistStr.append("gpfdist -d ");
			gpfdistStr.append(directory.getAbsolutePath());
			gpfdistStr.append(" -p ");
			gpfdistStr.append(GPFDIST_PORT);			
			gpfdistStr.append(" -l ").append(GPFDIST_ERROR_PATH).append("/load_errors_").append(GPFDIST_PORT).append("");
			
			boolean alreadyRunning = processMap.containsKey(tablename);
			
			// check if the port is opened
			if(alreadyRunning) {
				try{
					ServerSocket socket = new ServerSocket(GPFDIST_PORT);
					// process could connect and port is not up. Kill the process and start again.
					processMap.get(tablename).destroy();
					processMap.remove(tablename);
					alreadyRunning = false;
					socket.close();
				}
				catch(BindException e) {
					System.out.println("GPFDist process is already up for table "+tablename);
				}
			}
			
			// start GPFdist
			if (!alreadyRunning){
				System.out.println("Starting GPFDist: "+gpfdistStr.toString());
				Process process = Runtime.getRuntime().exec(gpfdistStr.toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line = reader.readLine();
				System.out.println(line);
				reader.close();
				if (line.indexOf("ERROR")==-1) {
					System.out.println("Started.");
					processMap.put(tablename, process);					
					Runtime.getRuntime().addShutdownHook(new Thread() {
						@Override
						public void run() {
							System.out.println("Killing GPFDist process for table "+tablename);
							processMap.get(tablename).destroy();
						}
					});
				}
				
				
			}
			
			// hostname
			Process process = Runtime.getRuntime().exec("hostname");
			String hostname = new BufferedReader(new InputStreamReader(process.getInputStream())).readLine();
			
			// create the external table if doesnt exist
			connection = helper.getGPConnectionSite2();
			DatabaseMetaData mData = connection.getMetaData();
			ResultSet rs = mData.getTables(null, null, tablename+"_in_"+hostname, null);
			if (!rs.next()) {
			
				System.out.println("Creating Readable External table for "+tablename);
				
				StringBuilder sql = new StringBuilder("CREATE EXTERNAL TABLE ");
				sql.append(tablename).append("_in_").append(hostname).append(" (LIKE ").append(tablename).append(") ");
				sql.append("LOCATION ('gpfdist://").append(hostname).append(":").append(GPFDIST_PORT);
				sql.append("/").append(tablename).append("') ");
				sql.append("FORMAT 'TEXT' ( DELIMITER '|' NULL ' ');");
				
				Statement stmt = connection.createStatement();
				try{
					stmt.executeUpdate(sql.toString());
				}
				catch(SQLException sqle) {
					System.out.println("Could not create external table for table "+tablename);
					System.out.println("That probably means this table was not created at this site yet. ");
					System.out.println(sqle.getMessage());					
				}
				
			}
			
			
			
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally {
			if (connection!=null) {
				try {
					connection.close();
				}catch(Exception e) {}
			}
		}
		
	}




	@Override
	public void execute(FunctionContext context) {

		System.out.println("Function called with arguments "+context.getArguments());
		String tableName = (String)context.getArguments();
		setup(tableName);
		context.getResultSender().lastResult(null);
	}




	@Override
	public String getId() {
		return "SetupGPBackupSiteFunction";
	}




	@Override
	public void init(Properties arg0) {
		// TODO Auto-generated method stub
		
	}
	
}
