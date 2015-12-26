package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : MaterialiationonHadoop
 * @Package : MRCube
 */

import java.util.Random;
import java.io.IOException;
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
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class MaterialiationOnHadoop {
	
	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";

	private static String connectionUrl;
	private static String jdbcDriverName;

	
	public static void main(String[] args) throws IOException {
		
		System.out.println("<=========== Materialization implementation Stats here.  ==========>");
		readMaterializationCubeRegions();
	}
	
	private static void loadConfiguration() throws IOException {
        InputStream input = null;
        try {
                String filename = "MRCube.conf";
                input = CubeLattice.class.getClassLoader().getResourceAsStream(filename);
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
	
public static void readMaterializationCubeRegions() throws IOException
{
	try {

		File fXmlFile = new File("Materialization_cuberegions.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
				
		//optional, but recommended
		//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
		doc.getDocumentElement().normalize();

		System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
				
		NodeList nList = doc.getElementsByTagName("materialization");
				
		System.out.println("-------------============---------------"+ nList.getLength());

		for (int temp = 0; temp < nList.getLength(); temp++) {

			Node nNode = nList.item(temp);
					
			System.out.println("\nCurrent Element :" + nNode.getNodeName());
					
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {

				Element eElement = (Element) nNode;

				System.out.println("Cube Region Id : " + eElement.getAttribute("id"));
				System.out.println("Region Name : " + eElement.getElementsByTagName("cuberegion").item(0).getTextContent());
				System.out.println("Region Storage Name : " + eElement.getElementsByTagName("regionstoragename").item(0).getTextContent());
				System.out.println("Is Materialize : " + eElement.getElementsByTagName("ismaterialize").item(0).getTextContent());
				System.out.println("Region Column Name : " + eElement.getElementsByTagName("regioncolumnname").item(0).getTextContent());

				String cubeRegion=eElement.getElementsByTagName("cuberegion").item(0).getTextContent();
				String regionStorageName=eElement.getElementsByTagName("regionstoragename").item(0).getTextContent();
				String isMaterialize=eElement.getElementsByTagName("ismaterialize").item(0).getTextContent();
				String regionColumnName =eElement.getElementsByTagName("regioncolumnname").item(0).getTextContent(); 
				if(Integer.parseInt(isMaterialize)==1)
				{
					System.out.println("if");
					MapReduceMaterializeCubeRegionsOnHadoop(regionColumnName,regionStorageName);
				}
				else
					System.out.println("else");
			}
		}
	    } catch (Exception e) {
	    		e.printStackTrace();
	    }
}	
	public static void MapReduceMaterializeCubeRegionsOnHadoop(String regionColumnName,String regionStorageName) throws IOException, ClassNotFoundException, TransformerException {
		
		System.out.println("\n============================================= Materialization process init... \n");
		loadConfiguration();

		Connection con = null;
		String query1;
		
		try {
			
			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();

			query1 = "SELECT "+regionColumnName+" FROM "+regionStorageName+"  ";
			
			System.out.println("Query1 : ==== "+query1);
			
			ResultSet rs1 = stmt.executeQuery(query1);
			
			String[] colsNames = regionColumnName.split(",");
			int colsLength = colsNames.length;
			
			for(int i =0; i < colsLength; i++)
			{
			    System.out.println(colsNames[i]);
			 
		    }
			
			try {
				
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

			// root elements
			Document doc = docBuilder.newDocument();
			Element rootElement = doc.createElement("cuberegion");
			doc.appendChild(rootElement);
					
			String storeXMLtagName=null;
			try {
				
				while (rs1.next()) 
				{
					// create random object
					Random randomno = new Random();
					 
					// check next int value  
					//System.out.println("Next int value: " + randomno.nextInt(10000));
					int randomNumber = randomno.nextInt(10000);   
					// staff elements
					Element staff = doc.createElement("regionid");
					rootElement.appendChild(staff);

					// set attribute to staff element
					Attr attr = doc.createAttribute("id");
					attr.setValue(Integer.toString(randomNumber));
					staff.setAttributeNode(attr);

					
					for(int i =0; i < colsLength; i++)
					{
					    //System.out.println(colsNames[i]);
					    //System.out.print("  "+rs1.getString(colsNames[i]));
					    
					    //firstname elements
						Element firstname = doc.createElement(colsNames[i]);
						storeXMLtagName=rs1.getString(colsNames[i]);
						if(rs1.getString(colsNames[i])==null)
							storeXMLtagName="NULL";
						
						firstname.appendChild(doc.createTextNode(storeXMLtagName));
						staff.appendChild(firstname);
				    }
					
					System.out.println(" Iteration ");
				}
				
				// write the content into xml file
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer = transformerFactory.newTransformer();
				DOMSource source = new DOMSource(doc);
				String fileName = "/home/cloudera/Desktop/Hadoop MR-Cube/Materialization/"+regionStorageName+".xml";
				StreamResult result = new StreamResult(new File(fileName));

				// Output to console for testing
				// StreamResult result = new StreamResult(System.out);

				transformer.transform(source, result);

				System.out.println("File saved !!");

				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }catch (ParserConfigurationException pce) {
				pce.printStackTrace();
		  }//DOM parser catch block ends here. 			
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
