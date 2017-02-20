package calcite.planner.physical.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.AggregationUtil;
import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ReductionKernel;

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
	int windowOffset = 0;
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
	
		if ((window == null) || ((window.getSize()==1) && (window.getSlide()==1))) {
			WindowType windowType = WindowType.ROW_BASED;
			int windowRange = 1024;
			int windowSlide = 1024;
			window = new WindowDefinition (windowType, windowRange, windowSlide);
		} 
		System.out.println("Window is : " + window.getWindowType().toString() + " with " + window.toString());
		LogicalAggregate aggregate = (LogicalAggregate) rel;
		
		QueryConf queryConf = new QueryConf (batchSize);
						
		AggregationUtil aggrHelper = new AggregationUtil();
		Pair<AggregationType [],FloatColumnReference []>  aggr = aggrHelper.getAggregationTypesAndAttributes(aggregate.getAggCallList());
		AggregationType [] aggregationTypes = aggr.left;
		FloatColumnReference [] aggregationAttributes = aggr.right;
		
		// error with rowtime column : should always be placed first in groupBy!!!
		// exclude rowtime, floor and ceil from group by attributes
		List<Integer> limitedGroupByList = new ArrayList<Integer>();
		int i = 0;
		for ( Integer groupby : aggregate.getGroupSet()){ 
			if (!(schema.getAttributeName(groupby).contains("rowtime")) && !(schema.getAttributeName(groupby).contains("FLOOR")) && !(schema.getAttributeName(groupby).contains("CEIL")) )
				limitedGroupByList.add(groupby);
			if ((schema.getAttributeName(groupby).contains("FLOOR")) || (schema.getAttributeName(groupby).contains("CEIL")) )
				this.windowOffset =  i; // works only with one floor or ceil
			i++;
		}
		ImmutableBitSet limitedGroupSet = ImmutableBitSet.builder().addAll(limitedGroupByList).build();
		Expression [] limitedGroupByAttributes = aggrHelper.getGroupByAttributes(limitedGroupSet, schema); 
		System.out.println("The referenced columns of group by are: " + limitedGroupSet.toList().toString());
		
		cpuCode = new Aggregation (window, aggregationTypes, aggregationAttributes, limitedGroupByAttributes);
		System.out.println(cpuCode);
		if (limitedGroupByAttributes == null) //maybe change it
			gpuCode = new ReductionKernel (window, aggregationTypes, aggregationAttributes, schema, batchSize);
		else
			gpuCode = new AggregationKernel (window, aggregationTypes, aggregationAttributes, limitedGroupByAttributes, schema, batchSize);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);				

		outputSchema = ((Aggregation) cpuCode).getOutputSchema();
		outputSchema = aggrHelper.createOutputSchema(aggregationTypes, aggregationAttributes, limitedGroupByAttributes, schema,outputSchema);	
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