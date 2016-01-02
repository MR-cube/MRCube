package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : NaiveAlgorithm
 * @Package : MRCube
<<<<<<< HEAD
 * @Purpose : To create data cubes.
=======
 * @Purpose : Naive algorithm used for to create datacubes on dimension attributes for further data mining algorithms. 
>>>>>>> origin/master
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
import java.util.Arrays;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom. Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class NaiveAlgorithm {
	
	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";

	private static String connectionUrl;
	private static String jdbcDriverName;
	private static String createStatement = "CREATE TABLE %s AS SELECT %s, COUNT(*) AS derivedCount FROM icc_bid_log GROUP BY %s";

	public static void main(String[] args) throws IOException {
		
		System.out.println("<=========== Naive Algorithm Implementation Stats here.  ==========>");
        loadConfiguration();
		Connection con = null;

		try {

			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();
 
			ResultSet rs = stmt.executeQuery("SELECT region_name,region_table_name FROM cube_lattice where rf_status='RF' AND  is_active='0' ");
			//stmt.executeQuery(sqlStatement);
			String columnName1,columnName2;
			try {
				while (rs.next())
				{
					System.out.println(rs.getString("region_name"));
					System.out.println(rs.getString("region_table_name"));
					columnName1 = rs.getString("region_table_name");
					columnName2 = rs.getString("region_name");
					
					createRegion(columnName1, columnName2);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("\n== Region Created Newly AP ======================");

		} catch (Exception e) {
			System.out.println("Warning: Region is already present AP");
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}		
	}
	
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
	 
	 public static void createRegion(String tableName, String columnName) throws IOException {
			String sqlStatement = String.format(createStatement, tableName, columnName, columnName);

			System.out.println("\n============================================= Naive Algo.");
			System.out.println(String.format("Creating Table %s (%s)", tableName, columnName));
			System.out.println("Running Query: " + sqlStatement);

	        loadConfiguration();

			Connection con = null;

			try {
				Class.forName(jdbcDriverName);

				con = DriverManager.getConnection(connectionUrl);

				Statement stmt = con.createStatement();

				//ResultSet rs = stmt.executeQuery("SELECT bid_id, count(*) As cnt FROM bid_master_l27 group by bid_id LIMIT 10");
				System.out.println(sqlStatement);
				stmt.executeQuery(sqlStatement);
				
				
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
			}
		}
	
}
