package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : ReadPropertiesXmlFile
 * @Package : MRCube
 */

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom. Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

public class ReadPropertiesXmlFile {

  public static void main(String argv[]) {

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
			
	System.out.println("------------------============----------"+nList.getLength());

	for (int temp = 0; temp < nList.getLength(); temp++) {

		Node nNode = nList.item(temp);
				
		System.out.println("\nCurrent Element :" + nNode.getNodeName());
				
		if (nNode.getNodeType() == Node.ELEMENT_NODE) {

			Element eElement = (Element) nNode;

			System.out.println("Cube Region Id : " + eElement.getAttribute("id"));
			System.out.println("Region Name : " + eElement.getElementsByTagName("regionname").item(0).getTextContent());
			System.out.println("RF Status : " + eElement.getElementsByTagName("rfstatus").item(0).getTextContent());
			System.out.println("Is Active : " + eElement.getElementsByTagName("isactive").item(0).getTextContent());

		}
	}
    } catch (Exception e) {
	e.printStackTrace();
    }
  }

}