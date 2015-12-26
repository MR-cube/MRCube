/**
 * 
 */
package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : PartialAlgebricMeasures
 * @Package : MRCube
 */

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class PartialAlgebricMeasures extends ValuePartitioning {
	
	public static void main(String[] args) throws IOException {
			
		String Input_PAM = "/home/cloudera/workspace/code/src/main/resources/PAM.json";
		JSONParser jsonParser = new JSONParser();
		
		try{
			
			FileReader fileReader = new FileReader(Input_PAM);
			JSONObject jsonObject = (JSONObject) jsonParser.parse(fileReader);
			JSONArray PAM_Attr = (JSONArray) jsonObject.get("PAM_Att");

			long table_count, total_records, reducer_limit;
			String result_table;
			boolean is_active;
			JSONArray columns, new_tables;
			int start_offset=0, limit=0;
			
			final Iterator it = PAM_Attr.iterator();
            while (it.hasNext()) {
                final JSONObject jsonPAM = (JSONObject) it.next();
                is_active = (boolean) jsonPAM.get("is_active");
                
                if (is_active) {        
                    result_table = (String) jsonPAM.get("result_table");
                    table_count = (long) jsonPAM.get("table_count");
                    columns = (JSONArray) jsonPAM.get("column_name");
                    new_tables = (JSONArray) jsonPAM.get("table_name");
                    total_records = (long) jsonPAM.get("total_records");
                    reducer_limit = (long) jsonPAM.get("reducer_limit");
                    
                	ExecutorService exService = Executors.newFixedThreadPool((int) table_count);
                	limit = ((int) total_records) / ((int) reducer_limit);
                	
                	for(int i = 0; i < new_tables.size(); i++){
                		
                		exService.execute(
                				new PAMEngine(JSONArraytoString(columns), 
                						new_tables.get(i).toString(), start_offset, limit));
                		
                		
                		start_offset = start_offset + limit;
                	}
                }
            }
			
		} catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
	}
	
	
    static void JsonLoad() {
    	String filename = "PAM.json";
    	InputStream filePath = PartialAlgebricMeasures.class.getClassLoader().getResourceAsStream(filename);
    	System.out.println(filePath);	
	}
	
    
    static String JSONArraytoString(JSONArray columns) {
    	String str_columns = Arrays.toString((columns.toArray()));
    	return str_columns.substring(1, str_columns.length() -1);
    }
    
}
