package com.project.MRCube;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
/**
 * @author : Amit Patange
 * @Class : MapReduceCubeRollup
 * @Package : MRCube
 */

public class MapReduceCubeRollup {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		
		   // Register driver and create driver instance
		   System.out.println("MapReduce jobs Operation initiated...");
		   
		   MapReduceCube_operation1();
		   MapReduceRollUP_operation1();
		
		   MapReduceCube_operation2();
		   MapReduceRollUP_operation2();
		
		   MapReduceCube_operation3();
		   MapReduceRollUP_operation3();
		
		   MapReduceCube_operation4();
		   MapReduceRollUP_operation4();
		
		   MapReduceCube_operation5();
		   MapReduceRollUP_operation5();
		
		   MapReduceCube_operation6();
		   MapReduceRollUP_operation6();
		   
		   System.out.println("MapReduce jobs cube and rollup Operation ended...");
		
	}
	
	public static void MapReduceCube_operation1() throws IOException {
	
		   String cubeStatement = "CREATE TABLE  cube_cou_reg_cit_der AS SELECT country,region,city,COUNT(*) AS groupCount FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(region,city),(country,city),(country),(region),(city),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Cube operation 1 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   connection.createStatement().executeQuery(cubeStatement);
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Cube operation 1 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.
	
	public static void MapReduceRollUP_operation1() throws IOException {
		
		   String rollupStatement = "CREATE TABLE  rollup_cou_reg_cit_der AS SELECT country,region,city,COUNT(*) AS groupCount FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(country),())";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Rollup operation 1 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   connection.createStatement().executeQuery(rollupStatement);
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Rollup operation 1 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.
	
	public static void MapReduceCube_operation2() throws IOException {
		
		   String cubeStatement = "CREATE TABLE  cube_reg_cit_log_der AS SELECT region,city,log_type, COUNT(*) AS groupCount FROM icc_bid_log GROUP BY region,city,log_type GROUPING SETS ((region,city,log_type),(region,city),(city,log_type),(region,log_type),(region),(city),(log_type),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Cube operation 2 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   System.out.println("After getting connection " + connection);
		   connection.createStatement().executeQuery("select * from default.icc_bid_log LIMIT 05");
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Cube operation 2 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.
	
	public static void MapReduceRollUP_operation2() throws IOException {
		
		   String rollupStatement = "CREATE TABLE  rollup_reg_cit_log_der AS SELECT region,city,log_type,COUNT(*) AS groupCount FROM icc_bid_log GROUP BY region,city,log_type GROUPING SETS ((region,city,log_type),(region,city),(region),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Rollup operation 2 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		
		   connection.createStatement().executeQuery(rollupStatement);
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Rollup operation 2 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.


	public static void MapReduceCube_operation3() throws IOException {
		
		   String cubeStatement = "CREATE TABLE  cube_cou_reg_cit_sbid_der AS SELECT country,region,city, SUM(bidding_price) AS sumBiddingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(region,city),(country,city),(country),(region),(city),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Cube operation 3 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   connection.createStatement().executeQuery(cubeStatement );
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Cube operation 3 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.
	
	public static void MapReduceRollUP_operation3() throws IOException {
		
		   String rollupStatement = "CREATE TABLE  rollup_cou_reg_cit_sbid_der AS SELECT country,region,city,SUM(bidding_price) AS sumBiddingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(country),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Rollup operation 3 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		
		   connection.createStatement().executeQuery(rollupStatement);
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Rollup operation 3 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.


	public static void MapReduceCube_operation4() throws IOException {
		
		   String cubeStatement = "CREATE TABLE  cube_cou_reg_cit_spay_der AS SELECT country,region,city, SUM(paying_price) AS sumPayingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(region,city),(country,city),(country),(region),(city),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Cube operation 4 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   connection.createStatement().executeQuery(cubeStatement );
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Cube operation 4 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.
	
	public static void MapReduceRollUP_operation4() throws IOException {
		
		   String rollupStatement = "CREATE TABLE  rollup_cou_reg_cit_spay_der AS SELECT country,region,city,SUM(paying_price) AS sumPayingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(country),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Rollup operation 4 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		
		   connection.createStatement().executeQuery(rollupStatement);
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Rollup operation 4 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.



	public static void MapReduceCube_operation5() throws IOException {
		
		   String cubeStatement = "CREATE TABLE  cube_cou_reg_cit_abid_der AS SELECT country,region,city, avg(bidding_price) AS avgBiddingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(region,city),(country,city),(country),(region),(city),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Cube operation 5 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   connection.createStatement().executeQuery(cubeStatement );
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Cube operation 5 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.
	
	public static void MapReduceRollUP_operation5() throws IOException {
		
		   String rollupStatement = "CREATE TABLE  rollup_cou_reg_cit_abid_der AS SELECT country,region,city,avg(bidding_price) AS avgBiddingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(country),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Rollup operation 5 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		
		   connection.createStatement().executeQuery(rollupStatement);
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Rollup operation 5 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.



	public static void MapReduceCube_operation6() throws IOException {
		
		   String cubeStatement = "CREATE TABLE  cube_cou_reg_cit_pay_der AS SELECT country,region,city, avg(paying_price) AS avgPayingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(region,city),(country,city),(country),(region),(city),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Cube operation 6 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   connection.createStatement().executeQuery(cubeStatement );
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Cube operation 6 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.
	
	public static void MapReduceRollUP_operation6() throws IOException {
		
		   String rollupStatement = "CREATE TABLE  rollup_cou_reg_cit_pay_der AS SELECT country,region,city,avg(paying_price) AS avgPayingPrice FROM icc_bid_log GROUP BY country,region,city GROUPING SETS ((country,region,city),(country,region),(country),()) ";
		
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Rollup operation 6 started with Map Reduce jobs.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		
		   connection.createStatement().executeQuery(rollupStatement);
		   /*
		    * while (resultSet.next()) {
			   System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		   }*/
		   System.out.println("Rollup operation 6 ends here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		   
	}//operation ends here.



}//class ends here.
