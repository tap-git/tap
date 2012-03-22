package tap.sample;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro. generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang.RandomStringUtils;



public class TestTool_GenerateAvroFile {

	/**
	 * @param args
	 */
	
	
	//generates an avro file with random data to be used for testing

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
	
	 
			 
	 Schema schema = Schema.parse(("{'type':'record', 'name':'key', 'fields': [ {'name': 'group', 'type': 'string'}," +
                     " {'name' : 'extra', 'type' : 'string'}," +
                     " {'name' : 'subsort', 'type' : 'string'}," +
                     " {'name' : 'value', 'type' : 'int'}]}")
                     .replaceAll("\\'", "\""));
		
     GenericRecord record = new GenericData.Record(schema);
     File file=new File("share/test_data2.avro");
     DatumWriter<GenericRecord> writer=new GenericDatumWriter<GenericRecord>(schema);
     DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<GenericRecord>(writer);
     dataFileWriter.create(schema, file);
     
     
     for(int i=0;i < 1000; i++)
     {
    	 
    	 //need to generate duplicates....
    	 String group =  RandomStringUtils.randomAlphabetic(3).toLowerCase();
    	 for(int j=0; j < 3; j++)
    	 {
    		 String extra = RandomStringUtils.randomAlphabetic(3).toLowerCase();
    		 
    		 for(int k=0;k < 3; k++)
    		 {
    			 String subsort = RandomStringUtils.randomAlphabetic(3).toLowerCase();
    			 record.put("group", group);
    			 record.put("extra", extra);
    			 record.put("subsort", subsort);
    			 record.put("value", 1);
    			 dataFileWriter.append(record);
    		 }
    	 }
    	 
    	
     }
	 
   
     dataFileWriter.close();

    
     
     //reading from the avro data file

     DatumReader<GenericRecord> reader= new GenericDatumReader<GenericRecord>();
     DataFileReader<GenericRecord> dataFileReader=new DataFileReader<GenericRecord>(file,reader);
     
     
   
     
     for(GenericRecord result : dataFileReader)
     {	 
    	
    	 System.out.println("group: " + result.get("group").toString() + " extra: " + 
    	 result.get("extra").toString() + " subsort: " + result.get("subsort").toString());
     	
     }

	}

}
