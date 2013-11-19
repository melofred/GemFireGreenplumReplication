package com.pivotal.gpdbreplication.gplog;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.pivotal.gpdbreplication.GemFireGPHelper;


public class LogFileTailListener implements  TailerListener{

	private String databaseName;
	
	private ClientCache cache;
	
	public LogFileTailListener(){
		
	}
	

	

	public String getDatabaseName() {
		return databaseName;
	}


	public void setDatabaseName(String databaseName) {
		this.databaseName = new StringBuilder("\"").append(databaseName).append("\"").toString();
	}


	@Override
	public void fileNotFound() {
		System.out.println("FILE NOT FOUND!!!!");
		
	}

	@Override
	public void fileRotated() {
		System.out.println("FILE ROTATED!!!!");
		/*
		tailer.stop();
		File file = scanDirForLogFile(dir);
		tailer = Tailer.create(file, this, delay, true);
		*/
	}

	@Override
	public void handle(String line) {
		//StringTokenizer tokenizer = new StringTokenizer(line,",");
		//if 
		
		/* EXEMPLO
		 * 
		 * 2013-10-21 15:44:26.043912 PDT, // log time
		 * "pivotal",	// user
		 * "template1",	// database
		 * p4844,		// ?
		 * th1381414656,	//?
		 * "::1",	//??
		 * "42385",	// ?
		 * 2013-10-21 15:44:25 PDT, // Operation time
		 * 1613,	// ?	
		 * con6,	// ?
		 * cmd3,	// ?
		 * seg-1,	// ?
		 * ,		// ?
		 * dx2,		// ?
		 * x1613,	// transaction ID
		 * sx1,		// ?
		 * "LOG",	// log type
		 * "00000",	//	?
		 * "statement: COMMIT"	// operation
		 * ,	// ?
		 * ,	// ?
		 * ,	// ?
		 * ,	// ?
		 * ,	// ?
		 * ,	// ?
		 * "COMMIT",	// statement as called 
		 * 0,	// ?
		 * ,	// ?
		 * "postgres.c",	// ?
		 * 1543,	// ?
		 * 
		 * 2013-10-21 16:08:00.531257 PDT,
		 * "pivotal",
		 * "pivotal",
		 * p5380,
		 * th1381414656,
		 * "[local]",
		 * ,
		 * 2013-10-21 15:52:44 PDT,
		 * 1623,
		 * con7,
		 * cmd27,
		 * seg-1,
		 * ,
		 * dx11,
		 * x1623,
		 * sx1,
		 * "LOG",
		 * "00000",
		 * "statement: CREATE TABLE my_ao_table  (empno int, name character varying(20), job character varying(20), deptno int) WITH (appendonly=true);",
		 * ,
		 * ,
		 * ,
		 * ,
		 * ,
		 * "CREATE TABLE my_ao_table  (empno int, name character varying(20), job character varying(20), deptno int) WITH (appendonly=true);",
		 * 0,
		 * ,
		 * "postgres.c",
		 * 1543,
		 * 
		 * 
		 */
		
		
		//String[] logFields = line.split(",");
		
		/*
		 * line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		 * 
		 * 
		 * String line = "foo,bar,c;qual=\"baz,blurb\",d;junk=\"quux,syzygy\"";
		*/
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ // enable comments, ignore white spaces
                ",                         "+ // match a comma
                "(?=                       "+ // start positive look ahead
                "  (                       "+ //   start group 1
                "    %s*                   "+ //     match 'otherThanQuote' zero or more times
                "    %s                    "+ //     match 'quotedString'
                "  )*                      "+ //   end group 1 and repeat it zero or more times
                "  %s*                     "+ //   match 'otherThanQuote'
                "  $                       "+ // match the end of the string
                ")                         ", // stop positive look ahead
                otherThanQuote, quotedString, otherThanQuote);

		String[] logFields = line.split(regex);
		 
		
		long id = GemFireGPHelper.getInstance().getNextTransactionLogId();
	
		cache = GemFireGPHelper.getInstance().getGemFireSite1Connection();
			if (logFields.length>20 && logFields[2].equals(databaseName) &&
					logFields[16].equals("\"LOG\"") 
					&& logFields[18].toLowerCase().startsWith("\"statement: ") 
					&& !logFields[18].toLowerCase().startsWith("\"statement: select")){
				
				// remove the "PDT " or "EST "
				String timestamp = logFields[0].substring(0, logFields[0].length()-4);			
				String statementToApply = logFields[24];
				String transactionId = logFields[14];
				//System.out.println("Statement to apply: " +statementToApply);
	
				
				Statement statement = new Statement();
				statement.setDatabaseName(databaseName);
				statement.setStatement(statementToApply);
				statement.setTransactionId(transactionId);
				statement.setTimestamp(timestamp);
				
				cache.getRegion("pending_statements").put(id, statement);
				
				
			}
			else if (logFields.length>20 && logFields[2].equals(databaseName) && logFields[16].equals("\"ERROR\"")){
	
				String timestamp = logFields[0].substring(0, logFields[0].length()-4);
				String statementToApply = logFields[24];
				String transactionId = logFields[14];
				//System.out.println("Statement to apply: " +statementToApply);
				
				Statement statement = new Statement();
				statement.setDatabaseName(databaseName);
				statement.setStatement(statementToApply);
				statement.setTransactionId(transactionId);
				statement.setTimestamp(timestamp);
				
				cache.getRegion("error_statements").put(id, statement);
							
			}
		
	}

	@Override
	public void handle(Exception e) {
		System.out.println("EXCEPTION!!!!");
		e.printStackTrace();
	}

	@Override
	public void init(Tailer arg0) {
		// TODO Auto-generated method stub
		
	}


}
