package com.pivotal.gpdbreplication.out;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.pivotal.gpdbreplication.GemFireGPConfigurator;
import com.pivotal.gpdbreplication.GemFireGPHelper;

public abstract class ConfiguredAsyncEventListener implements AsyncEventListener, Declarable {

	protected GemFireGPHelper helper;
	protected GemFireGPConfigurator configurator;

	
	protected ConfiguredAsyncEventListener() {
		helper = GemFireGPHelper.getInstance();
		configurator = GemFireGPConfigurator.getInstance();
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(Properties arg0) {
		// TODO Auto-generated method stub
		
	}




}
