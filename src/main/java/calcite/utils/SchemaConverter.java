package calcite.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;

import calcite.planner.SaberRelDataTypeSystem;
import uk.ac.imperial.lsds.saber.ITupleSchema;

public class SchemaConverter {
	SchemaPlus schema;	
	
	public SchemaConverter(SchemaPlus schema){
		this.schema = schema;		
	}
	
	/* Create Saber's tables from a given Calcite schema*/
	public List<Pair<String,ITupleSchema>> convert(){
		List<Pair<String,ITupleSchema>> tablesList = new ArrayList<Pair<String,ITupleSchema>>();
		Set<String> tables = schema.getTableNames();
		for (String table : tables){
			Table calTable = schema.getTable(table);			
		    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(SaberRelDataTypeSystem.SABER_REL_DATATYPE_SYSTEM);						
			List<RelDataTypeField> fields = calTable.getRowType(typeFactory).getFieldList();
			ITupleSchema saberSchema = new SaberSchema().createTable(fields);
			System.out.println("Schema : " + saberSchema.getSchema());
			
			tablesList.add(new Pair <String,ITupleSchema>(table, saberSchema));
		}				
		return  tablesList;
	}
	
	
	/* Fill the tables with dummy data for testing*/
	public Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> setMockInput(List<Pair<String,ITupleSchema>> tableList){
		Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> tablesMap = new HashMap<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>>();
		for (Pair<String,ITupleSchema> p : tableList){			
			Pair<byte [],ByteBuffer> mockData = new SaberSchema().fillTable(p.right);
			tablesMap.put(p.left, new Pair <ITupleSchema,Pair<byte [],ByteBuffer>>(p.right,mockData));
		}				
		return tablesMap;
	}
	
	/* Create an input source for streaming data*/
	public Map<String, Pair<ITupleSchema,byte []>> setInputSource(){
		return null;
	}

}
