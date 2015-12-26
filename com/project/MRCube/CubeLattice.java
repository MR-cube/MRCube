package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : CubeLattice
 * @Description : Full cube lattice and analyzing regions are fetched from full cube lattice.
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

public class CubeLattice extends PartialAlgebricMeasures {
	
	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";
	/**
	 * JDBC connection is created with coudera impala database which is based on top of apache hadoop.
	 * drivers area loaded through hadoop framework.
	 * */
	
	private static String connectionUrl;
	private static String jdbcDriverName;
	private static String deleteStatement = "DROP TABLE dimension_att_table";
	private static String createStatement = "CREATE TABLE dimension_att_table ( dimension STRING, status string)";
	private static String createStatement2 = "CREATE TABLE full_cube_lattice ( region_name STRING, rf_status string, is_active string)";
	private static String createStatement3 = "CREATE TABLE cube_lattice ( region_id STRING,region_name STRING,region_table_name STRING,vp_factor STRING, rf_status string, is_active string)";
	private static String insertStatement = "insert into dimension_att_table values (%s,%s)";
	private static String insertStatement2 = "insert into full_cube_lattice values (%s,%s,%s,%s)";
	private static String latticeRegionName;
	
	/**
	 * All required statements are declared here.
	 * */

	/**	loadconfiguration defined here.  */
        private static void loadConfiguration() throws IOException {
                InputStream input = null;
                try {
                        String filename = "MRCube.conf";
                        input = CubeLattice.class.getClassLoader().getResourceAsStream(filename);
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
                        }
                }
        }

	public static void main(String[] args) throws IOException {

		//Get file from resources folder
		ClassLoader classLoader = CubeLattice.class.getClassLoader();
		File file = new File(classLoader.getResource("DimensionAttribute.conf").getFile());

		
		createDimensionAttributeOnHDFS();
		
		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] regions = line.split(",", 2);
				
				setMRCubeDimensions(regions[0], regions[1].trim());
			}

			scanner.close(); 
			/** dimension config files reading and processing completed. also scanner is also closed
			 * Now need to create combination of all the dimension attributes to create full cube lattice.
			*/
			
			MapReduceCreateFullCubeLatticeStorage();
			getDimensionAttributes();
			
			//File cubeLatticeFile = new File(classLoader.getResource("CubeLattice.conf").getFile());
			MapReduceCreateValidCubeLatticeStorage();
			ReadPropertiesCubeLatticeXML();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}
	
	
	public static void createDimensionAttributeOnHDFS()  throws IOException {
		
		System.out.println("\n============================================= Setting Dimension Att List \n");
		loadConfiguration();

		Connection con = null;
		
		
		try {
		
			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();
			
			//stmt.executeQuery(deleteStatement);
			//System.out.println("\n"+createStatement);
			System.out.println("\n New dimension storage is generated.");
			stmt.executeQuery(createStatement);
			
		 System.out.println("\n== Dimension Att list generated. ======================");
			
		}catch (Exception e) {
			System.out.println("Low Warning: Dimension table is created. But query doesn't generate any response.  "+e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		
		
		}
  }
		
	public static void MapReduceCreateFullCubeLatticeStorage()  throws IOException {
		
		System.out.println("\n============================================= Full Cube lattice table \n");
		loadConfiguration();

		Connection con = null;
		
		
		try {
		
			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();
			
			//stmt.executeQuery(deleteStatement);
			//System.out.println("\n"+createStatement2);
			System.out.println("\n Full cube lattice is created as storage for cube regions.");
			stmt.executeQuery(createStatement2);
			
		 System.out.println("\n== full cube lattice table generated. ======================");
			
		}catch (Exception e) {
			System.out.println("Warning: Full Cube Lattice table is created. But query doesn't generate any response.  "+e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		
		
		}
  }
	
	public static void setMRCubeDimensions(String columnValue1, String columnValue2)  throws IOException 	{
		
		String sqlStatement = String.format(insertStatement, columnValue1, columnValue2);
		System.out.println("\n============================================= Setting Dimension Att List \n");
		loadConfiguration();

		String impalaiQuery = "insert into dimension_att_table values ('"+columnValue1+"','"+columnValue2+"')";
		
		Connection con = null;
		
		
		try {
		
			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();
			
			System.out.println("\n== Stored new dimension attribute as "+columnValue1);
			stmt.executeQuery(impalaiQuery);
			
		 System.out.println("\n== Generating dimension attributes list. ======================");
			
		}catch (Exception e) {
			System.out.println("W : dimension generated. But no return response.  "+e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		
		
		}
	}
	
	
	public static void MapReduceSetFullCubeLattice(String columnValue1, String columnValue2,String columnValue3)  throws IOException 	{
		
		String sqlStatement = String.format(insertStatement, columnValue1, columnValue2, columnValue3);
		System.out.println("\n============================================= Setting Dimension Att List \n");
		loadConfiguration();

		String impalaiQuery = "insert into full_cube_lattice values ('"+columnValue1+"','"+columnValue2+"','"+columnValue3+"')";
		
		Connection con = null;
		
		
		try {
		
			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();
			
			//System.out.println("\n== "+impalaiQuery);
			System.out.println("\n== Created new cube region as :  "+columnValue1);
			stmt.executeQuery(impalaiQuery);
			
		 System.out.println("\n== impalaiQuery ==");
			
		}catch (Exception e) {
			System.out.println("W : dimension generated. But no return response.  "+e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		
		
		}
	}
	
	public static void getDimensionAttributes() throws IOException {
	
		System.out.println("\n============================================= Setting Dimension Att List \n");
		loadConfiguration();

		String impalaSQuery1 = null;
		String impalaSQuery2 = null;
		Connection con = null;
		String totalDimensionCount=null;
		String dimensionName=null;
		int i=0;
		try {

			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();

			impalaSQuery1 = " SELECT COUNT(dimension) AS totalDimension FROM dimension_att_table where status='0' ";
			ResultSet rs1 = stmt.executeQuery(impalaSQuery1);
			
			try {
				
				while (rs1.next())
				{
					totalDimensionCount = rs1.getString("totalDimension");
					System.out.println("Total Dimension "+totalDimensionCount);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
			impalaSQuery2 = " SELECT dimension FROM dimension_att_table where status='0' ";
			ResultSet rs2 = stmt.executeQuery(impalaSQuery2);
				
			String[] dimensionStack = new String[Integer.parseInt(totalDimensionCount)];
			
			try {
				
				while (rs2.next())
				{
					System.out.println(i+" ===  "+rs2.getString("dimension"));
					dimensionStack[i]=rs2.getString("dimension");
					i++;
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/**
			  // Print all the array elements
		      for (int j = 0; j < dimensionStack.length; j++) {
		         System.out.println(dimensionStack[j] + " ********** ");
		      }
			*/
			System.out.println("\n== dimension array created for processing ======================");
			
	        try {
	        	generateCubeLatticeMapReduceCube(dimensionStack);
	        	//combine(dimensionStack, Integer.parseInt(r));
	        }catch (Exception e) {
				System.out.println("Warning: Cube Lattice formation process failed  "+e.getMessage());
			} finally {
				try {
					//in case database connection/file con is remained opened.
				} catch (Exception e) {
					// swallow
				}
			}

		} catch (Exception e) {
			System.out.println("Warning: Dimension array failure "+e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
	}
	
	public static void generateCubeLatticeMapReduceCube(String[] cubeLattice) throws IOException {
		
		 	String r = "2";
		 	//int[] arr = {1, 2, 3, 4, 4, 5};
	        //Arrays.sort(arr);
	        try {
	        	
	        	for (int k = 1; k < 4; k++) {
	        		combine(cubeLattice,k);//Integer.parseInt(r)
			      }
	        	
	        }catch (Exception e) {
				System.out.println("Warning: Cube Lattice formation process failed  "+e.getMessage());
			} finally {
				try {
					//in case database connection/file con is remained opened.
				} catch (Exception e) {
					// swallow
				}
			}
	}
	
	
	private static void combine(String[] arr, int r) throws IOException {
        String[] res = new String[r];
        doCombine(arr, res, 0, 0, r);
    }



    private static void doCombine(String[] arr, String[] res, int currIndex, int level, int r) throws IOException {
        if(level == r){
            printArray(res);
            return;
        }
        for (int i = currIndex; i < arr.length; i++) {
            res[level] = arr[i];
            doCombine(arr, res, i+1, level+1, r);
            //way to avoid printing duplicates
            if(i < arr.length-1 && arr[i] == arr[i+1]){
                i++;
            }
        }
    }

    private static void printArray(String[] res) throws IOException {
    	
    	latticeRegionName="";
        for (int i = 0; i < res.length; i++) {
        	latticeRegionName+=res[i] + ",";
            //System.out.print(res[i] + ",");
        }
        latticeRegionName = latticeRegionName.replaceAll(",$", "");
        System.out.println(latticeRegionName);
        MapReduceSetFullCubeLattice(latticeRegionName,"RF","0");
        
    }
    public static void ReadPropertiesCubeLatticeXML() throws IOException {
    	
    	 try {

    			File fXmlFile = new File("CubeLattice.xml");
    			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    			Document doc = dBuilder.parse(fXmlFile);
    					
    			//optional, but recommended
    			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
    			doc.getDocumentElement().normalize();

    			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
    					
    			NodeList nList = doc.getElementsByTagName("cuberegion");
    					
    			System.out.println("----------------------------");

    			for (int temp = 0; temp < nList.getLength(); temp++) {

    				Node nNode = nList.item(temp);
    						
    				System.out.println("\nCurrent Element :" + nNode.getNodeName());
    						
    				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

    					Element eElement = (Element) nNode;

    					System.out.println("Cube Region Id : " + eElement.getAttribute("id"));
    					System.out.println("Region Name : " + eElement.getElementsByTagName("regionname").item(0).getTextContent());
    					System.out.println("Region Table Name : " + eElement.getElementsByTagName("regiontablename").item(0).getTextContent());
    					System.out.println("RF Status : " + eElement.getElementsByTagName("rfstatus").item(0).getTextContent());
    					System.out.println("Is Active : " + eElement.getElementsByTagName("isactive").item(0).getTextContent());

    					String column1=eElement.getElementsByTagName("regionname").item(0).getTextContent();
    					String column2=eElement.getElementsByTagName("rfstatus").item(0).getTextContent();
    					String column3=eElement.getElementsByTagName("isactive").item(0).getTextContent();
    					String column4=eElement.getElementsByTagName("regiontablename").item(0).getTextContent();
    					String column5=eElement.getAttribute("id");
    					String column6=eElement.getElementsByTagName("vpfactor").item(0).getTextContent();
    					
    					//if(column3=="0")
    						setValidCubeLatticeOnHDFS(column1,column2,column3,column4,column5,column6);
    				}
    			}
    		    } catch (Exception e) {
    			e.printStackTrace();
    		    }
    }
    
    public static void MapReduceCreateValidCubeLatticeStorage() throws IOException {
    	
    	System.out.println("\n============================================= Cube lattice table \n");
		loadConfiguration();

		Connection con = null;
		
		
		try {
		
			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();
			
			//stmt.executeQuery(deleteStatement);
			//System.out.println("\n"+createStatement3);
			System.out.println("\n Valid Cube Lattice is generated : ");
			stmt.executeQuery(createStatement3);
			
		 System.out.println("\n== Valid cube lattice table generated. ======================");
			
		}catch (Exception e) {
			System.out.println("Warning: Valid Cube Lattice table is created. But query doesn't generate any response.  "+e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		
		
		}
    }
    
    public static void setValidCubeLatticeOnHDFS(String col1,String col2,String col3,String col4,String col5,String col6) throws IOException {
    	
    	System.out.println("\n============================================= Setting Dimension Att List \n");
		loadConfiguration();

		String impalaiQuery = "insert into cube_lattice values ('"+col5+"','"+col1+"','"+col4+"','"+col6+"','"+col2+"','"+col3+"')";
		
		Connection con = null;
		
		
		try {
		
			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();
			
			//System.out.println("\n== "+impalaiQuery);
			System.out.println("\n== Generating cube lattice regions as :  "+col1);
			stmt.executeQuery(impalaiQuery);
			
		 System.out.println("\n== Generated Valid Cube Lattice. ======================");
			
		}catch (Exception e) {
			System.out.println("Warning : Generated Valid Cube Lattice. But no return response.  "+e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		
		
		}
    }
	
}


