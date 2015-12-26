package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : BatchAreas
 * @Description : Batchareas formed and loaded in HDFS for in memory processing.
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
import java.util.Arrays;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom. Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class BatchAreas extends MaterialiationOnHadoop {
	
	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";

	private static String connectionUrl;
	private static String jdbcDriverName;
	
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
 }//function ends here.
	

		public static void main(String[] args) throws IOException {
		
				System.out.println("Batch Area process initiated.");
				/**
				 * Before starting to form batch areas first we are invalidating hadoop metadata on hcatalog. 
				 * */
			
				try {
					invalidateMetadata();
					prepareCubeLatticeForBatches();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		}

	 
	 public static void invalidateMetadata() throws IOException {
		 
		 System.out.println("\n Invalidating HDFS metadata.");
		 loadConfiguration();

		 Connection con = null;	
			try {
			
				Class.forName(jdbcDriverName);

				con = DriverManager.getConnection(connectionUrl);

				Statement stmt = con.createStatement();
		
				stmt.executeQuery("invalidate metadata");
						
			}catch (Exception e) {
				System.out.println("-Invalidate metadata  "+e.getMessage());
			} finally {
				try {
					con.close();
				} catch (Exception e) {
					// swallow
				}
			
			
			}
	 }
	 
	 public static void prepareCubeLatticeForBatches() throws IOException, ClassNotFoundException {
		 
		 	System.out.println("\n Preparing cube lattice for forming batch areas. \n");
			loadConfiguration();
			//Integer.parseInt(totalDimensionCount)
			String[] batchStack = new String[100];
			String[][] b  = new String[20][100]; //Batch Area b stack is generated.
			Connection con = null;
			
			try {

				Class.forName(jdbcDriverName);
				con = DriverManager.getConnection(connectionUrl);
				Statement stmt = con.createStatement();

				String iQuery = "SELECT region_id,region_name,region_table_name,vp_factor,rf_status,is_active FROM cube_lattice where is_active='0' order by rf_status DESC ";
				ResultSet rs1 = stmt.executeQuery(iQuery);
					
				int i=0;
				int j=0;
				while (rs1.next())
				{
					System.out.println("Cube Region Name  "+rs1.getString(1));
					batchStack[i] =rs1.getString(1); 
					b[i][j]=rs1.getString(1);
					i++;
				}
				//String[][] phones = {{"Apple", "iPhone"}, {"Samsung", "Galaxy"}, {"Sony", "Xperia"}};

				System.out.println("Real batch values : " + Arrays.deepToString(batchStack));
				//System.out.println("Real batch values : " + Arrays.deepToString(b[1]));
				//System.out.println("Real batch values : " + Arrays.deepToString(b[2]));
				 
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					//in case database connection/file con is remained opened.
				} catch (Exception e) {
					// swallow
				}
			}
	 }
}