package com.pivotal.gpdbreplication.out;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

public class InvokeSetupFunctionListener extends CacheListenerAdapter implements Declarable{

	@Override
	public void afterRegionCreate(RegionEvent event) {
	}

	@Override
	public void afterCreate(EntryEvent event) {

		Cache cache = CacheFactory.getAnyInstance();
		String table = (String)event.getNewValue();
		Execution execution = FunctionService.onRegion(cache.getRegion("GPFDistPorts"));
		System.out.println("Executing function SetupGPBackupSiteFunction for table "+table);
		execution.withArgs(table).execute("SetupGPBackupSiteFunction");
		System.out.println("Done.");
		
	
	}

	@Override
	public void init(Properties arg0) {
		// TODO Auto-generated method stub
		
	}

	
	
	
}
