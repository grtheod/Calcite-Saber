package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.SaberRule;
import calcite.planner.physical.rules.util.SaberAggregateUtil;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;


public class SaberAggregateRule implements SaberRule{
	public static final String usage = "usage: Aggregate";

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
	int windowOffset = -1;
	int batchSize;
	
	public SaberAggregateRule(ITupleSchema schema,RelNode rel, int queryId, long timestampReference, WindowDefinition window, int batchSize){
		this.rel = rel;
		this.schema = schema;
		this.window = window;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.batchSize = batchSize;
	}
	
	public void prepareRule() {
		
		// TODO: Decide which will be the window size if no definition is given.
		if ((window == null) || ((window.getSize()==1) && (window.getSlide()==1))) {
			WindowType windowType = WindowType.ROW_BASED;
			int windowRange = 1024;
			int windowSlide = 1024;
			window = new WindowDefinition (windowType, windowRange, windowSlide);
		} 
		
		System.out.println("Window is : " + window.getWindowType().toString() + " with " + window.toString());
		LogicalAggregate aggregate = (LogicalAggregate) rel;
		QueryConf queryConf = new QueryConf (batchSize);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		SaberAggregateUtil aggr = new SaberAggregateUtil(aggregate.getAggCallList(), aggregate.getGroupSet(), batchSize, schema, window);
		aggr.build();
		
		operators.add(aggr.getOperator());
		cpuCode = aggr.getCpuCode();
		gpuCode = aggr.getGpuCode();
		windowOffset = aggr.getWindowOffset();
		outputSchema = aggr.getOutputSchema();
		
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);				
	}
	public ITupleSchema getOutputSchema(){
		return this.outputSchema;
	}
	
	public Query getQuery(){
		return this.query;
	}
	
	public IOperatorCode getCpuCode(){
		return this.cpuCode;
	}
	
	public IOperatorCode getGpuCode(){
		return this.gpuCode;
	}

	public WindowDefinition getWindow() {
		return window;
	}

	public WindowDefinition getWindow2() {
		return null;
	}
	
	public Pair<Integer, Integer> getWindowOffset() {				
		return new Pair<Integer, Integer>(this.windowOffset,-1);
	}
	
}