package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : PAMEngine
 * @Package : MRCube
 */

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class PAMEngine implements Runnable{

	private String columns;
	private String table_name;
	private int start_offset;
	private int limit;
	
	PAMEngine(String columns, String table_name, int start_offset, int limit) {
		this.columns = columns;
		this.table_name = table_name;
		this.start_offset = start_offset;
		this.limit = limit;
	}
	
	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";
	private static String connectionUrl;
	private static String jdbcDriverName;
	
	private static String createStatement = "CREATE TABLE %s AS SELECT %s FROM icc_bid_log order by %s limit %03d offset %03d";
	
	private static void loadConfiguration() throws IOException {
        InputStream input = null;
        try {
                String filename = "MRCube.conf";
                input = RegionsGenerator.class.getClassLoader().getResourceAsStream(filename);
                Properties prop = new Properties();
                prop.load(input);

                connectionUrl = prop.getProperty(CONNECTION_URL_PROPERTY);
                jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
        } finally {
                try {
                        if (input != null)
                                input.close();
                } catch (IOException e) {
                        // nothing to do
                }
        }
	} 
	
	
	@Override
	public void run(){		
		String sqlStatement = String.format(createStatement, table_name, columns, columns, limit, start_offset);

		System.out.println("\n=============================================");
		System.out.println(String.format("Creating Table %s (%s)", table_name, columns));
		System.out.println("Running Query: " + sqlStatement);

		Connection con = null;

		try {
			loadConfiguration();
			Class.forName(jdbcDriverName);
			con = DriverManager.getConnection(connectionUrl);
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStatement);
			System.out.println(sqlStatement + " Done");
		} catch (Exception e) {
			System.out.println("Warning: table is already present");
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
	}

}
