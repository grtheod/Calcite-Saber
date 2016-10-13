package calcite.utils;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public class SaberSchema {
	int tuplesPerInsert = 32768; // get it as an attribute	
	
	/* Create a schema in Saber from a given list of Calcite's DataTypes.*/
	public ITupleSchema createTable(List<RelDataTypeField> fields ){
		int numberOfAttributes = fields.size();
		int [] offsets = new int [numberOfAttributes + 1];
		offsets[0] = 0;
		int tupleSize = 8;
		int i;
		for (i = 1; i < numberOfAttributes + 1; i++) {
			offsets[i] = tupleSize;
			tupleSize += 4;
		}
		
		/*Only the first column is set as LONG. I should fix it.*/
		ITupleSchema schema = new TupleSchema (offsets, tupleSize);
		schema.setAttributeType(0,  PrimitiveType.LONG);
		for (i = 1; i < numberOfAttributes + 1; i++) {
			if (fields.get(i-1).getType().toString().equals("FLOAT")) {
				schema.setAttributeType(i, PrimitiveType.FLOAT);
				schema.setAttributeName(i, fields.get(i-1).getName());
			} else {
				schema.setAttributeType(i, PrimitiveType.INT);
				schema.setAttributeName(i, fields.get(i-1).getName());
			}
		}		
		
		return schema;
	}
	
	/* Create a mock table from a given schema*/
	public Pair<byte [],ByteBuffer> fillTable(ITupleSchema schema){
		
		int tupleSize = schema.getTupleSize();
		int numberOfAttributes = schema.numberOfAttributes() - 1;
		//System.out.println("tupleSize = "+tupleSize);
		byte [] data = new byte [tupleSize * tuplesPerInsert];
		
		ByteBuffer b = ByteBuffer.wrap(data);
		/* Fill the buffer */
		int number = 0;
		while (b.hasRemaining()) {
			b.putLong (1);
			for (int i = 0; i < numberOfAttributes; i ++)
				//number = (int) (Math.random() * 10 + 1);				
				b.putInt(1);
				b.put(schema.getPad());
		}
		
		return new Pair<byte [],ByteBuffer>(data,b);		
	} 
	
}
