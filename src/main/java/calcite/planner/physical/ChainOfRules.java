package calcite.planner.physical;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.WindowDefinition;

public class ChainOfRules {
	Query query;
	ITupleSchema outputSchema;
	WindowDefinition window;
	byte [] data1, data2;
	boolean isFirst;
	boolean isJoin;
	boolean hasMore;
	int windowOffset;
	
	ChainOfRules(Query query, ITupleSchema outputSchema, WindowDefinition window, byte [] data, boolean isJoin, boolean isFirst, int windowOffset) {
		this.query = query;
		this.outputSchema = outputSchema;
		this.data1 = data;
		this.window = window;
		this.isFirst = isFirst;
		this.isJoin = isJoin;
		this.windowOffset = windowOffset;
	}
	
	ChainOfRules(Query query, ITupleSchema outputSchema, WindowDefinition window, byte [] data1 , byte [] data2, boolean isJoin,boolean isFirst, boolean hasMore) {
		this.query = query;
		this.outputSchema = outputSchema;
		this.window = window;
		this.data1 = data1;
		this.data2 = data2;
		this.isFirst = isFirst;
		this.isJoin = isJoin;
		this.hasMore = hasMore;
	}
	
	public void addRule(Query query, ITupleSchema outputSchema, WindowDefinition window) {
		this.query = query;
		this.outputSchema = outputSchema;
		this.window = window;
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
	
	public WindowDefinition getWindow(){
		return this.window;
	}
	
	public int getWindowOffset() {
		return this.windowOffset;
	}
}
