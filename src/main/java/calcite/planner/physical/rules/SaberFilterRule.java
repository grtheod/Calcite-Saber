package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.SaberRule;
import calcite.planner.physical.rules.util.SaberFilterUtil;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;


public class SaberFilterRule implements SaberRule {
	
	public static final String usage = "usage: Filter";
	
	RelNode rel;
	WindowDefinition window;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	int queryId = 0;
	long timestampReference = 0;
	int batchSize;
	
	public SaberFilterRule(ITupleSchema schema, RelNode rel, int queryId , long timestampReference, WindowDefinition window, int batchSize){
		this.rel = rel;
		this.schema = schema;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.window = window;
		this.batchSize = batchSize;
	}
	
	public void prepareRule() {
				
		WindowType windowType = (window!=null) ? window.getWindowType() : WindowType.ROW_BASED;
		long windowRange = (window!=null) ? window.getSize() : 1;
		long windowSlide = (window!=null) ? window.getSlide() : 1;
		
		QueryConf queryConf = new QueryConf (batchSize);		
		window = new WindowDefinition (windowType, windowRange, windowSlide);	
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		
		LogicalFilter logFilter = (LogicalFilter) rel;
		
		SaberFilterUtil filter = new SaberFilterUtil(logFilter.getCondition(), batchSize, schema);
		filter.build();
		
		operators.add(filter.getOperator());
		cpuCode = filter.getCpuCode();
		gpuCode = filter.getGpuCode();
		outputSchema = filter.getOutputSchema();
	    
		WindowDefinition executionWindow = new WindowDefinition (WindowType.ROW_BASED, 1, 1);
		System.out.println("Window is : " + executionWindow.getWindowType().toString() + " with " + executionWindow.toString());
		
		query = new Query (queryId, operators, schema, executionWindow, null, null, queryConf, timestampReference);
		//resize the window according to possible changes from input
		window = new WindowDefinition (windowType, windowRange, windowSlide);
	}
	
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}
	
	public Query getQuery() {
		return this.query;
	}
	
	public IOperatorCode getCpuCode(){
		return this.cpuCode;
	}
	
	public IOperatorCode getGpuCode(){
		return this.gpuCode;
	}

	public WindowDefinition getWindow() {
		return this.window;
	}

	public WindowDefinition getWindow2() {
		return null;
	}
	
	public Pair<Integer, Integer> getWindowOffset() {
		return new Pair<Integer, Integer>(0,0);
	}
	
}
