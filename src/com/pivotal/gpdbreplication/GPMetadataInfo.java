package com.pivotal.gpdbreplication;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class GPMetadataInfo {

	static GPMetadataInfo instance;

	static class TableInfo{
		String schemaName;
		String tableName;
		String tableType;
		
	}
	
	static class ColumnInfo{
		String name;
		int type;
		String strType;
		int size;
		int decimalDigits;
		boolean nullable;
		String defaultValue;
		public ColumnInfo(String name, int type, String strType, int size,
				int decimalDigits, boolean nullable, String defaultValue) {
			super();
			this.name = name;
			this.type = type;
			this.strType = strType;
			this.size = size;
			this.decimalDigits = decimalDigits;
			this.nullable = nullable;
			this.defaultValue = defaultValue;
		}

		
		
		
	}
	
	GemFireGPHelper helper;
	
	
	public static synchronized GPMetadataInfo getInstance() {
		if (instance==null) {
			instance = new GPMetadataInfo();
			instance.helper = GemFireGPHelper.getInstance();
		}
		return instance;
	}
	
	private Connection getGPConnection() throws SQLException{
		
		//return DriverManager.getConnection("jdbc:postgresql://localhost:5432/pivotal","pivotal","gopivotal");
		return helper.getGPConnection();
	}


	
	public ColumnInfo[] getColumns(String table) throws SQLException{
		Connection conn = getGPConnection();
		System.out.println(" Showing columns for table "+table);
		ResultSet rs = conn.getMetaData().getColumns(null, null, table, null);
		
		System.out.println("\nColumns for "+table);
		
		ArrayList<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		while (rs.next()){
			/*
			System.out.print(rs.getString(1)+" "); //catalog
			System.out.print(rs.getString(2)+" "); //schema
			System.out.print(rs.getString(3)+" "); // table
			System.out.print(rs.getString(4)+" "); // column
			System.out.print(rs.getString(5)+" "); // type (from sql.Types)
			System.out.print(rs.getString(6)+" "); // type name
			System.out.print(rs.getString(7)+" "); // size
			System.out.print(rs.getString(8)+" "); // buffer length (ignore)
			System.out.print(rs.getString(9)+" "); // decimal digits
			System.out.print(rs.getString(10)+" ");// radix 
			System.out.print(rs.getString(11)+" "); // nullable
			System.out.print(rs.getString(12)+" "); // remarks
			System.out.print(rs.getString(13)+" "); // default value
			System.out.print(rs.getString(14)+" "); // unused
			System.out.print(rs.getString(15)+" "); // unused
			System.out.print(rs.getString(16)+" "); // max size for chars
			System.out.print(rs.getString(17)+" "); // index of the column (starting at 1) at the table
			System.out.print(rs.getString(18)+" "); // IS_NULLABLE 
			
			System.out.print("\n");
			*/
			columnList.add(new ColumnInfo(rs.getString(4), 
					rs.getInt(5), rs.getString(6), rs.getInt(7), rs.getInt(9), 
					rs.getInt(11)==1, rs.getString(13)));
			
		}
		return columnList.toArray(new ColumnInfo[columnList.size()]);
		
	}

	
	public String[] getGPAppendOnlyTables() throws SQLException{
	
		Connection conn = getGPConnection();
		ArrayList<String> aoTables = new ArrayList<String>();
		
		PreparedStatement pstmt = conn.prepareStatement("select get_ao_distribution(?)");	
		
		DatabaseMetaData md = conn.getMetaData();
		/*
		ResultSet rs = md.getTableTypes();
		System.out.println("\nTable types:");
		while (rs.next()){
			System.out.println(rs.getString(1));
		}
		System.out.println("\nCatalogs:");
		rs = md.getCatalogs();
		while (rs.next()){
			System.out.println(rs.getString(1));
		}

		System.out.println("\nTables:");
		*/
		ResultSet rs = md.getTables("pivotal","public", null , new String[]{"TABLE"});
		while (rs.next()){
			/*
			System.out.print(rs.getString(1)+" ");
			System.out.print(rs.getString(2)+" ");
			System.out.print(rs.getString(3)+" ");
			System.out.print(rs.getString(4)+" ");
			System.out.print(rs.getString(5)+" ");
			System.out.print("\n");
			*/
			pstmt.setString(1, rs.getString(3));
			try{
				ResultSet rs2 = pstmt.executeQuery();
				aoTables.add(rs.getString(3));
			}
			catch(Exception e){
				// ignore. Means table is not AO
				//System.out.println(rs.getString(3)+" Table is not AO");
			}
			
			
		}
		return (String[])aoTables.toArray(new String[aoTables.size()]);
		
	}
	
	


	
}
