package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
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
	
	
	public SaberAggregateRule(ITupleSchema schema,RelNode rel, int queryId, long timestampReference, WindowDefinition window){
		this.rel = rel;
		this.schema = schema;
		this.window = window;
		this.queryId = queryId;
		this.timestampReference = timestampReference;	
	}
	
	public void prepareRule() {
	
		int batchSize = 1048576;
		if ((window == null) || ((window.getSize()==1) && (window.getSlide()==1))) {
			WindowType windowType = WindowType.ROW_BASED;
			int windowRange = 1024;
			int windowSlide = 1024;
			window = new WindowDefinition (windowType, windowRange, windowSlide);
		} 
		System.out.println(window.toString());
		LogicalAggregate aggregate = (LogicalAggregate) rel;
		
		QueryConf queryConf = new QueryConf (batchSize);
						
		AggregationUtil aggrHelper = new AggregationUtil();
		Pair<AggregationType [],FloatColumnReference []>  aggr = aggrHelper.getAggregationTypesAndAttributes(aggregate.getAggCallList());
		AggregationType [] aggregationTypes = aggr.left;
		FloatColumnReference [] aggregationAttributes = aggr.right;
		
		//error with rowtime column : should always be placed first in groupBy!!!
		//ImmutableBitSet groupSet = aggregate.getGroupSet().except(ImmutableBitSet.of(0));
		Expression [] groupByAttributes = aggrHelper.getGroupByAttributes(aggregate.getGroupSet(), schema);
		Expression [] limitedGroupByAttributes = null; //without rowtime column
		if (!(groupByAttributes == null) && (groupByAttributes.length > 1)) {
			int i;
			limitedGroupByAttributes = new Expression [groupByAttributes.length - 1];
			for (i=1; i < groupByAttributes.length; i++)
				limitedGroupByAttributes[i-1] = groupByAttributes[i]; 
		}
		
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
	
	public int getWindowOffset() {				
		return 0;
	}
	
}