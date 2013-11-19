package com.pivotal.gpdbreplication;

public class OffSetTest {

	public static void main(String[] args) {
		GemFireGPHelper helper = GemFireGPHelper.getInstance();
		
		long offset = helper.getOffset("my_ao_table2");
		long batchsize = helper.getBatchSize("my_ao_table2");
		
		System.out.println(">>> OFFSET FOR my_ao_table2: "+offset);
		System.out.println(">>> BATCH SIZE FOR my_ao_table2: "+batchsize);
		
		
	}
	
}
