package calcite.planner.physical;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;

public class ChainOfRules {
	Query query;
	ITupleSchema outputSchema;
	int whiteSpaces = 0;
	byte [] data;
	boolean isJoin;

	ChainOfRules(int wS , Query query, ITupleSchema outputSchema, byte [] data , boolean isJoin) {
		this.whiteSpaces = wS;
		this.query = query;
		this.outputSchema = outputSchema;
		this.isJoin = isJoin;
		this.data = data;
	}
	
	public void addRule(int wS , Query query, ITupleSchema outputSchema) {
		this.whiteSpaces = wS;
		this.query = query;
		this.outputSchema = outputSchema;
	}
		
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}
	
	public Query getquery() {
		return this.query;
	}
	
	public int getWhitespaces() {
		return this.whiteSpaces;
	}
	
	public byte [] getData(){
		return this.data;
	}
	
	public boolean getFlag(){
		return this.isJoin;
	}
}
