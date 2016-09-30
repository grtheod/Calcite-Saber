package calcite.utils;

import java.nio.ByteBuffer;

import org.apache.calcite.util.Pair;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public class SaberSchema {
	int numberOfAttributes;
	int tuplesPerInsert = 32768; // get it as an attribute	
	
	public SaberSchema(int numberOfAttributes){
		this.numberOfAttributes = numberOfAttributes;		
	}
	
	public ITupleSchema createTable(){
		int [] offsets = new int [numberOfAttributes + 1];
		offsets[0] = 0;
		int tupleSize = 8;
		int i;
		for (i = 1; i < numberOfAttributes + 1; i++) {
			offsets[i] = tupleSize;
			tupleSize += 4;
		}
		
		ITupleSchema schema = new TupleSchema (offsets, tupleSize);
		schema.setAttributeType(0,  PrimitiveType.LONG);
		for (i = 1; i < numberOfAttributes + 1; i++) {
			schema.setAttributeType(i, PrimitiveType.INT);
		}
		
		
		return schema;
	}
	
	public Pair<byte [],ByteBuffer> fillTable(ITupleSchema schema){
		
		int tupleSize = schema.getTupleSize();
		//System.out.println("tupleSize="+tupleSize);
		byte [] data = new byte [tupleSize * tuplesPerInsert];
		
		ByteBuffer b = ByteBuffer.wrap(data);
		/* Fill the buffer */
		int number = 0;
		while (b.hasRemaining()) {
			b.putLong (1);
			for (int i = 0; i < numberOfAttributes; i ++)
				//number = (int) (Math.random() * 10 + 1);				
				b.putInt(1);
		}
		
		return new Pair<byte [],ByteBuffer>(data,b);		
	} 
	
}
