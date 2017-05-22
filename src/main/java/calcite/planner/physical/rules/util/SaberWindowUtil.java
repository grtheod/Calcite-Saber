package calcite.planner.physical.rules.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.AggregationUtil;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ReductionKernel;

public class SaberWindowUtil implements SaberRuleUtil{

	List<AggregateCall> aggCall;
	ImmutableBitSet groupSet;
	WindowDefinition window;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	int batchSize;
	QueryOperator aggOperator;
	int windowOffset = 0;
	int windowBarrier = 0;
	
	public SaberWindowUtil (List<AggregateCall> aggCall, ImmutableBitSet groupSet, int batchSize, ITupleSchema schema, WindowDefinition window) {
		this.aggCall = aggCall;
		this.groupSet = groupSet;
		this.batchSize = batchSize;
		this.schema = schema;
		this.batchSize = batchSize;
		this.window = window;		
	}
	
	@Override
	public void build() {
		
		// create window here??
		
		AggregationUtil aggrHelper = new AggregationUtil();
		Pair<AggregationType [],FloatColumnReference []>  aggr = aggrHelper.getAggregationTypesAndAttributes(aggCall);
		AggregationType [] aggregationTypes = aggr.left;
		FloatColumnReference [] aggregationAttributes = aggr.right;
		
		//Expression [] groupByAttributes = aggrHelper.getGroupByAttributes(windowAgg.groups.get(0).keys, schema);
		int numberOfGroupByAttributes = groupSet.length(); // groupByAttributes.length;
		// exclude rowtime, floor and ceil from group by attributes
		List<Integer> limitedGroupByList = new ArrayList<Integer>();
		for ( Integer groupby : groupSet){
			if (!(schema.getAttributeName(groupby).contains("rowtime")) && !(schema.getAttributeName(groupby).contains("FLOOR")) && !(schema.getAttributeName(groupby).contains("CEIL")) )
				limitedGroupByList.add(groupby);
		}
		int inputAttrs = schema.numberOfAttributes();
		for (int i = 0; i<inputAttrs; i++) {
			//System.out.println((schema.getAttributeName(i).toString()));
			if ((schema.getAttributeName(i).contains("FLOOR")) || (schema.getAttributeName(i).contains("CEIL"))){
					this.windowBarrier = i;
					break;
			}
		}
/*		if (windowBarrier == 0) 
			windowBarrier++;*/
		
		System.out.println("The window barrier is:"+this.windowBarrier);
		ImmutableBitSet limitedGroupSet = ImmutableBitSet.builder().addAll(limitedGroupByList).build();
		Expression [] limitedGroupByAttributes = aggrHelper.getGroupByAttributes(limitedGroupSet, schema); 
		System.out.println("The referenced columns of group by are: " + limitedGroupSet.toList().toString());
		
		cpuCode = new Aggregation (window, aggregationTypes, aggregationAttributes, limitedGroupByAttributes);
		System.out.println(cpuCode);
		if (limitedGroupByAttributes == null) 
			gpuCode = new ReductionKernel (window, aggregationTypes, aggregationAttributes, schema, batchSize);
		else
			gpuCode = new AggregationKernel (window, aggregationTypes, aggregationAttributes, limitedGroupByAttributes, schema, batchSize);
		
		aggOperator = new QueryOperator (cpuCode, gpuCode);		
				
		windowOffset = this.schema.numberOfAttributes() - numberOfGroupByAttributes;
		
		outputSchema = ((Aggregation) cpuCode).getOutputSchema();		
		outputSchema = aggrHelper.createOutputSchema(aggregationTypes, aggregationAttributes, limitedGroupByAttributes, schema,outputSchema);	
	}

	@Override
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}

	@Override
	public QueryOperator getOperator() {
		return this.aggOperator;
	}

	@Override
	public IOperatorCode getCpuCode() {
		return this.cpuCode;
	}

	@Override
	public IOperatorCode getGpuCode() {
		return this.gpuCode;
	}

	@Override
	public WindowDefinition getWindow() {
		return this.window;
	}

	@Override
	public WindowDefinition getWindow2() {
		return null;
	}
	
	public int getWindowOffset() {
		return this.windowOffset;
	}
	
	public int getWindowBarrier() {
		return this.windowBarrier;
	}

}
