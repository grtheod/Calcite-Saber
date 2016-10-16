package calcite.planner.physical;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;

public class ChainOfRules {
	Query query;
	ITupleSchema outputSchema;
	byte [] data1, data2;
	boolean isFirst;
	boolean isJoin;
	boolean hasMore;

	ChainOfRules(Query query, ITupleSchema outputSchema, byte [] data , boolean isJoin, boolean isFirst) {
		this.query = query;
		this.outputSchema = outputSchema;
		this.data1 = data;
		this.isFirst = isFirst;
		this.isJoin = isJoin;
	}
	
	ChainOfRules(Query query, ITupleSchema outputSchema, byte [] data1 , byte [] data2, boolean isJoin,boolean isFirst, boolean hasMore) {
		this.query = query;
		this.outputSchema = outputSchema;
		this.data1 = data1;
		this.data2 = data2;
		this.isFirst = isFirst;
		this.isJoin = isJoin;
		this.hasMore = hasMore;
	}
	
	public void addRule(Query query, ITupleSchema outputSchema) {
		this.query = query;
		this.outputSchema = outputSchema;
	}
		
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}
	
	public Query getQuery() {
		return this.query;
	}
		
	public byte [] getData(){
		return this.data1;
	}

	public byte [] getData2(){
		return this.data2;
	}	
	
	public boolean getFlag(){
		return this.isJoin;
	}
		
	public boolean getIsFirst(){
		return this.isFirst;
	}
	
	public boolean getHasMore(){
		return this.hasMore;
	}
	
}
