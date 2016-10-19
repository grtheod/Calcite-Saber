package calcite.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Pair;

import uk.ac.imperial.lsds.saber.ITupleSchema;

public class DataGenerator {
	DataGen dataGenerator;
	Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> tablesMap;
	
	
	/** Default data generator. */
	public DataGenerator() {
		this.dataGenerator = new DataGen();
	}
	
	public DataGenerator setSchema (SchemaPlus schema, boolean useMockData) {
		this.dataGenerator.schema = schema;
		SchemaConverter schemaConverter = new SchemaConverter(schema);
		List<Pair<String,ITupleSchema>> tablesList = schemaConverter.convert();
		if (useMockData){
			this.tablesMap = schemaConverter.setMockInput(tablesList);
		} else {
			/* give the input source*/
		}
		return this;
	} 
	
	public DataGenerator build(){
		return this;
	}
	
	public Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> getTablesMap(){
		return this.tablesMap;
	}
	
}
