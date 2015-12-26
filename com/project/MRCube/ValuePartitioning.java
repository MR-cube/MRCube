package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : ValuePartitioning
 * @Description : Perform VP only on groups that are likely to be reducer-unfriendly and create a partition factor. Use sampling approach
 * 				 to perform cube computation.
 * @Parameters - N : sample size
 * 				 R : Reducer can handle records
 * 			    |D| : total no of records.
 * 			     C : Reducer limit
 *               s : Sample Count 
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

public class ValuePartitioning extends BatchAreas {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	private static String connectionUrl;
	private static String jdbcDriverName;
	
	public static int N;
	/** Sample Size data. */
	public static int s;
	/** Sample Count */
	public static int c=1000000;
	/** c denote reducer limit (Max number of tuples a single reducer can handle.) 
	 * and setting that reducer handles 1M tuples on cluster. */
	public static int d;
	/** |D| denotes total number of data. */
	
	public static int r = c/d;
	/** r denotes r=(c/|D|).  */
	
	public static String sample01;
	/** We takes three random samples and perform VP. */

	public static String sample02;
	/** We takes three random samples and perform VP. */

	public static String sample03;
	/** We takes three random samples and perform VP. */

	public static void main(String[] args) throws IOException {
		
		System.out.println("Value Partitioning MR jobs started.");
		/** Here we get value of |D| so we can use this for sampling approach*/
		d=getTotalNumberRows(); 
		System.out.println("|D| =  "+d);
		dropPreviousSamples();
		ProcessValuePartitioningRUFRegions();
	
	}//Main ends here.
	
	public static int getTotalNumberRows() throws IOException {
		
		   String rowsStatement = "select COUNT(*) AS totalrows from icc_bid_log LIMIT 01 ";
		   String totalRows=null;
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Creating sampling process.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   
		   ResultSet resultSet = connection.createStatement().executeQuery(rowsStatement);
		    while (resultSet.next()) {
			   //System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		    	totalRows=resultSet.getString(1);
		   }//while ends.
		    d=Integer.valueOf(totalRows);
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }//catch ends here.
		return d;
	}
	
	public static void dropPreviousSamples() throws IOException {
		
		 String sample01= "drop table sample01 ";
		 String sample02= "drop table sample02 ";
		 String sample03= "drop table sample03 ";
		 
		 String sampleGroup01= "drop table samplegroup01 ";
		 String sampleGroup02= "drop table samplegroup02 ";
		 String sampleGroup03= "drop table samplegroup03 ";
		 
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Dropping previous sampling results. and getting ready for new value partitioning sampling.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   
		   connection.createStatement().executeQuery(sample01);
		   connection.createStatement().executeQuery(sample02);
		   connection.createStatement().executeQuery(sample03);
		   
		   connection.createStatement().executeQuery(sampleGroup01);
		   connection.createStatement().executeQuery(sampleGroup02);
		   connection.createStatement().executeQuery(sampleGroup03);
		   
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }//catch ends here.
	}
	
	 public static void createSamplingGroups(String tableName,String columnNames,String sampleCount) throws IOException {
		 
		   sampleCount=Integer.toString(Integer.parseInt(sampleCount)/10);
		   
		   /** We are only scanning 10% of the whole data and taking 3 samples to check cube region is reducer unfriendly or not. */
		   String sampleStatement01 = "CREATE TABLE sample01 AS select  "+columnNames+"  from icc_bid_log TABLESAMPLE("+sampleCount+" ROWS) t ";
		   String sampleStatement02 = "CREATE TABLE sample02 AS select  "+columnNames+"  from icc_bid_log TABLESAMPLE("+sampleCount+" ROWS) t ";
		   String sampleStatement03 = "CREATE TABLE sample03 AS select  "+columnNames+"  from icc_bid_log TABLESAMPLE("+sampleCount+" ROWS) t ";
			
		   /** Creating cube groups from sampled input from base input location. And 
		    * now we are going to apply Bernoullis trials therom.  */
		   String sampleGroupStatement01 = "CREATE TABLE samplegroup01 AS select  "+columnNames+",COUNT(*) AS groupCount  from icc_bid_log GROUP BY "+columnNames+"   ";
		   String sampleGroupStatement02 = "CREATE TABLE samplegroup02 AS select  "+columnNames+",COUNT(*) AS groupCount  from icc_bid_log GROUP BY "+columnNames+"   ";
		   String sampleGroupStatement03 = "CREATE TABLE samplegroup03 AS select  "+columnNames+",COUNT(*) AS groupCount  from icc_bid_log GROUP BY "+columnNames+"   ";
			
		   
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Started to create sampling process.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		
		   System.out.println("Sample01 "+sampleStatement01);
		   System.out.println("SampleGroup01 "+sampleGroupStatement01);
		   
		   connection.createStatement().executeQuery(sampleStatement01);
		   connection.createStatement().executeQuery(sampleStatement02);
		   connection.createStatement().executeQuery(sampleStatement03);
		   
		   connection.createStatement().executeQuery(sampleGroupStatement01);
		   connection.createStatement().executeQuery(sampleGroupStatement02);
		   connection.createStatement().executeQuery(sampleGroupStatement03);
		   System.out.println("VP. Sampling loop closing here.");

		   System.exit(1);
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
 }
	 
	 
	 
	 public static void ProcessValuePartitioningRUFRegions() throws IOException {
		 
		 int groupCount=0;
		 String tableName=null;
		 String columnNames = null;
		 String sampleCount=null;
		 
		 String regionId,regionName,regionTableName,vpFactor;
		 String cubeRUFStatement = "SELECT region_id,region_name,region_table_name,vp_factor FROM cube_lattice where rf_status='RUF' AND  is_active='0' ";
			
		   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Sampling reducer unfriendly regions.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   ResultSet resultSet = connection.createStatement().executeQuery(cubeRUFStatement);
		   
		     while (resultSet.next()) {
			     System.out.println(resultSet.getString(1) + "  =  " + resultSet.getString(2)+"   =   "+resultSet.getString(3) + "  =   " + resultSet.getString(4));
		    	 regionId=resultSet.getString(1);
		    	 regionName=resultSet.getString(2);
		    	 regionTableName=resultSet.getString(3);
		    	 vpFactor=resultSet.getString(4);
		    	 createSamplingGroups(regionTableName,regionName,Integer.toString(d));
		    	 checkReducerFriendlyStatus();
		   }//while ends here.
		    
		    /** Checking cube group is satisfies G >= 0.75rN condition */
		   //validateCubeGroupsForRUF(); 
		   System.out.println("VP process jobs here.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
		 
	 }	 
	 
public static void checkReducerFriendlyStatus() throws IOException {
	  
	String firstSampleRUFStatus=null;
	String secondSampleRUFStatus=null;
	String thirdSampleRUFStatus=null;
	
	String RUFStatement1="SELECT COUNT(*) AS sampleSizeCount FROM samplegroup01 LIMIT 01";
	String RUFStatement2="SELECT COUNT(*) AS sampleSizeCount FROM samplegroup02 LIMIT 01";
	String RUFStatement3="SELECT COUNT(*) AS sampleSizeCount FROM samplegroup03 LIMIT 01";
	
	Boolean one = false ;
	Boolean second = false;
	Boolean third = false;
	
	
	try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Sampling reducer unfriendly regions.");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   ResultSet resultSet1 = connection.createStatement().executeQuery(RUFStatement1);
		   ResultSet resultSet2 = connection.createStatement().executeQuery(RUFStatement2);
		   ResultSet resultSet3 = connection.createStatement().executeQuery(RUFStatement3);
		   
		     while (resultSet1.next()) {
			     //System.out.println(resultSet1.getString(1));
		    	 firstSampleRUFStatus = resultSet1.getString(1);
		    	 
		   }//while ends here.
		     
		     while (resultSet2.next()) {
			     //System.out.println(resultSet1.getString(1));
		    	 secondSampleRUFStatus = resultSet2.getString(1);
		    	 
		   }//while ends here.
		     
		     while (resultSet3.next()) {
			     //System.out.println(resultSet1.getString(1));
		    	 thirdSampleRUFStatus = resultSet3.getString(1);
		    	 
		   }//while ends here.
		     
		     /**
		      * Now we have sample size N into respective local variables.
		      * We are benchmarking the reducer limit that only those no of tuples
		      * a reducer can handle on this single/multi cluster node.
		      * */
		    System.out.println("sampling results : "+firstSampleRUFStatus+"     "+secondSampleRUFStatus+"        "+thirdSampleRUFStatus);
		    /*
		     * Created cube groups from reducer unfriendly cube regions we have taken sampling groups to check whole cube 
		     * region to RUF.
		     * */
		    
		    if(validateCubeGroupsForRUF(firstSampleRUFStatus))
		    {
		    	one=true;	
		    }//if ends here.
		    
		    if(validateCubeGroupsForRUF(secondSampleRUFStatus))
		    {
		    	second=true;	
		    }//if ends here.
		    
		    if(validateCubeGroupsForRUF(thirdSampleRUFStatus))
		    {
		    	third=true;
		    }//if ends here.
		    
		    if(one==true || second==true || third==true)
		    {
		    	//if further action want to take if condition satisfies for UF.
		    }
		   System.out.println("MR Job to check cube region is reducer unfriendly and returns with close statement1.");
		   }catch (Exception e) {
			   //e.printStackTrace();
			   //System.exit(1);
		   }
	
}
public static boolean validateCubeGroupsForRUF(String getSampleSize) throws IOException {
	
	/**
	 * We can perform here value partitioning condition.
	 * Gi >= 0.75rN -----------------------------------------------------------------(01)
	 * or else we can also further increase partition factor.
	 * */
	double  groupStat=0;
	boolean vpregionRUF=true;
	
	try {
			groupStat = 0.75 * r * N;
			vpregionRUF=getFinalRUFStatus(getSampleSize,groupStat); 
			if(!vpregionRUF)
			{
				/**
				 * We further adjust partition factor so we can reducer reducer size and re initiate
				 * all the proc to get partition factor and along region with that. 
				 * */
				vpregionRUF=adjustPartitionFactor();
				
				return vpregionRUF;
			}
			
	}catch(Exception e){
		e.printStackTrace();
		System.out.println("Multiplication error.");
	}//catch ends here.
	/**
	 * Storing groupStat and returning reducer unfriendly status finally.
	 * */
	
	return true;
			   
   }

public static boolean getFinalRUFStatus(String arg1,double arg2) throws IOException {
	int val1 = Integer.parseInt(arg1);
	double val2 = arg2;
	
	if(val1>val2)
		return true;
	else
		return false;
}

public static boolean adjustPartitionFactor() throws IOException
{
	return true;
}
	 
}