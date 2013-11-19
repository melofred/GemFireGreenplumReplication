package com.pivotal.gpdbreplication.gplog;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.io.input.Tailer;

public class LogFilePoller {
	
	
	private static File scanDirForLogFile(String dir){
		
		File directory = new File(dir);
		
		File[] logFiles = directory.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if (name.endsWith("csv")) return true;
				return false;
			}
		});
		
		Arrays.sort(logFiles, new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				return Long.valueOf(o2.lastModified()).compareTo(o1.lastModified());
			}
			
			
			
		});
		
		// last modified .csv log file
		return logFiles[0];
	}


	private String dir;
	private long delay;
	private LogFileTailListener listener;
	
	
	
	public String getDir() {
		return dir;
	}


	public void setDir(String dir) {
		this.dir = dir;
	}


	public long getDelay() {
		return delay;
	}


	public void setDelay(long delay) {
		this.delay = delay;
	}


	public LogFileTailListener getListener() {
		return listener;
	}


	public void setListener(LogFileTailListener listener) {
		this.listener = listener;
	}


	public void startPolling(){
		
		System.out.println("Starting up poller for directory: " +dir);
		
		File file = scanDirForLogFile(dir);
		
		Tailer.create(file, listener, delay, true, true);

	}
	

	public LogFilePoller(String dir, long delay, LogFileTailListener listener){
		
		this.dir = dir;
		this.delay = delay;
		this.listener = listener;
	
		
		
	}
	
}
