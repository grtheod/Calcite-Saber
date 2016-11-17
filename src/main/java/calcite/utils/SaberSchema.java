package calcite.utils;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public class SaberSchema {
	int tuplesPerInsert = 32768; // get it as an attribute	
	
	/* Create a schema in Saber from a given list of Calcite's DataTypes.*/
	public ITupleSchema createTable(List<RelDataTypeField> fields ){
		int numberOfAttributes = fields.size();
		int [] offsets = new int [numberOfAttributes];
		offsets[0] = 0;
		int tupleSize = 0;
		int i;
		for (i = 0; i < numberOfAttributes; i++) {
			offsets[i] = tupleSize;
			if ((fields.get(i).getType().toString().equals("FLOAT")) || (fields.get(i).getType().toString().equals("INTEGER"))) 
				tupleSize += 4;
			else
				tupleSize += 8;
		}
		
		
		ITupleSchema schema = new TupleSchema (offsets, tupleSize);
		schema.setAttributeType(0,  PrimitiveType.LONG);
		for (i = 0; i < numberOfAttributes; i++) {
			if (fields.get(i).getType().toString().equals("INTEGER")) {
				schema.setAttributeType(i, PrimitiveType.INT);
				schema.setAttributeName(i, fields.get(i).getName());
			} else
			if (fields.get(i).getType().toString().equals("FLOAT")) {
				schema.setAttributeType(i, PrimitiveType.FLOAT);
				schema.setAttributeName(i, fields.get(i).getName());
			} else { //If an attribute is neither integer nor float, it is defined as long
				schema.setAttributeType(i, PrimitiveType.LONG);
				schema.setAttributeName(i, fields.get(i).getName());
			}
		}		
		
		return schema;
	}
	
	/* Create a mock table from a given schema*/
	public Pair<byte [],ByteBuffer> fillTable(ITupleSchema schema){
		
		int tupleSize = schema.getTupleSize();
		int numberOfAttributes = schema.numberOfAttributes() - 1;
		byte [] data = new byte [tupleSize * tuplesPerInsert];
		
		ByteBuffer b = ByteBuffer.wrap(data);
		/* Fill the buffer */
		while (b.hasRemaining()) {
			b.putLong (1);
			for (int i = 0; i < numberOfAttributes; i ++) {
				
				if (schema.getAttributeType(i + 1).equals(PrimitiveType.LONG)) 
					b.putLong (1);
				else
				if (schema.getAttributeType(i + 1).equals(PrimitiveType.FLOAT)) 
					b.putFloat (1);
				else  
					b.putInt(1);
			}
			b.put(schema.getPad());
		}
		
		return new Pair<byte [],ByteBuffer>(data,b);		
	} 
	
}
