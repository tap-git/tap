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


public class TestTool_ViewAvroFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		  File file=new File("/tmp/out/part-00000.avro");
		  GenericDatumReader reader = new GenericDatumReader();
		  DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
		  
		  for(GenericRecord r : dataFileReader)
		  {
			  System.out.println(r.get("group2") + " " + r.get("extra2") + " " + r.get("subsort"));
		  }
		  
		
		

	}

}
