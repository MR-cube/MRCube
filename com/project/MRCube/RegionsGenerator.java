package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : RegionsGenerator
 * @Description : Cube regions are generated.
 * @Package : MRCube
 */

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class RegionsGenerator {
	
	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";
	/**
	 * JDBC connection is created with coudera impala database which is based on top of apache hadoop.
	 * drivers area loaded through hadoop framework.
	 * */
	
	private static String connectionUrl;
	private static String jdbcDriverName;
	private static String createStatement = "CREATE TABLE %s AS SELECT %s, COUNT(*) FROM icc_bid_log GROUP BY %s";
	/**
	 * Statements are declared here.
	 * */

        private static void loadConfiguration() throws IOException {
                InputStream input = null;
                try {
                        String filename = "MRCube.conf";
                        input = RegionsGenerator.class.getClassLoader().getResourceAsStream(filename);
                        /** Configure file is loaded to read connection string. */
                        Properties prop = new Properties();
                        prop.load(input);
                        /** Properties are loaded here. */
                        connectionUrl = prop.getProperty(CONNECTION_URL_PROPERTY);
                        jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
                } finally {
                        try {
                                if (input != null)
                                        input.close();
                        } catch (IOException e) {
                                // nothing to do
                        }/** Exception are catched here. */
                }
        }

	public static void main(String[] args) throws IOException {

		//Get file from resources folder
		ClassLoader classLoader = RegionsGenerator.class.getClassLoader();
		File file = new File(classLoader.getResource("Regions.conf").getFile());
		/** Cube Region are loaded and in the process of generating. */
		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				/** Cube Region strings are reading line by line. */
				String line = scanner.nextLine();
				String[] regions = line.split(",", 2);
				
				createRegion(regions[0], regions[1].trim());
				/** Cube Region are generated here. we are also providing storage name on apache hadoop. */
			}

			scanner.close();
			/** File reader scanner is closed. */
		} catch (IOException e) {
			e.printStackTrace();
		}/** IOExceptions are catched. */
		
	}
	
	
	public static void createRegion(String tableName, String columnName) throws IOException {
		String sqlStatement = String.format(createStatement, tableName, columnName, columnName);
		/** Cube Region statement are formated and displayed in jConsole.  */
		System.out.println("\n=============================================");
		System.out.println(String.format("Creating Table %s (%s)", tableName, columnName));
		System.out.println("Running Query: " + sqlStatement);

        loadConfiguration();
        /** Cloudera impala configuration is loaded. */

		Connection con = null;
		/** Cloudera impala connection string. */
		try {

			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();

			//ResultSet rs = stmt.executeQuery("SELECT bid_id, count(*) As cnt FROM bid_master_l27 group by bid_id LIMIT 10");
			stmt.executeQuery(sqlStatement);
			/** Hadoop query is executed to generate cube regions. */
			/*try {
				while (rs.next())     
					System.out.println(rs.getString("bid_id"));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
			

			System.out.println("\n== Region Created Newly AP ======================");

		} catch (Exception e) {
			System.out.println("Warning: Region is already present AP");
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}/** Connection closes. */
	}/** Function ends. */
	
	
	
}/** Class ends. */
