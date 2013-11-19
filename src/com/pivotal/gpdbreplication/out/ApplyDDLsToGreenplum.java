package com.pivotal.gpdbreplication.out;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;

public class ApplyDDLsToGreenplum extends ConfiguredAsyncEventListener {


	
	@Override
	public boolean processEvents(List<AsyncEvent> events) {
		
		Connection conn = null;
		try {
		
			conn = helper.getGPConnectionSite2();
			for (AsyncEvent e:events) {
				
				String commandString = e.getDeserializedValue().toString();
				
				// remove the start and ending quotes
				commandString = commandString.substring(1,commandString.length()-1);
						
				System.out.println("PREPARING TO EXECUTE "+commandString);
				Statement stmt = conn.createStatement();
				stmt.executeUpdate(commandString);
				
			}
			conn.commit();
			return true;
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally{
			if (conn!=null) {
				try {conn.close();}catch(Exception e) {};
			}
		}
		
	}


}
