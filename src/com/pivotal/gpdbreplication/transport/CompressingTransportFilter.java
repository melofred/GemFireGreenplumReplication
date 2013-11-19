package com.pivotal.gpdbreplication.transport;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class CompressingTransportFilter implements GatewayTransportFilter, Declarable {

	
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public InputStream getInputStream(InputStream in) {
		try {
			return new CompressedInputStream(in);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally {
		}
	}

	@Override
	public OutputStream getOutputStream(OutputStream out) {
		try {
			return new CompressedOutputStream(out);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally {
		}
	}

	@Override
	public void init(Properties props) {
		System.out.println(">> Creating the Compressing Gateway Transport Filter...");
		
		//bufferSize = Integer.parseInt(props.getProperty("buffer-size",String.valueOf(DEFAULT_BUFFER_SIZE)));
		
	}

}
