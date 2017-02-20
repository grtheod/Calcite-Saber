package calcite.planner.physical.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexLiteral;
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

public class SaberWindowRule implements SaberRule{
	public static final String usage = "usage: Window";

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
	int numberOfGroupByAttributes;
	int windowBarrier=0;
	int batchSize;
	
	public SaberWindowRule(ITupleSchema schema,RelNode rel, int queryId, long timestampReference, WindowDefinition window, int batchSize){
		this.rel = rel;
		this.schema = schema;
		this.window = window;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.batchSize = batchSize;
	}
	
	public void prepareRule() {
	
		int windowSlide = 1;
		if (!(window == null))
			windowSlide = (int) window.getSlide(); 
		
		/*At this moment, a window has only one group of aggregate functions.*/
		LogicalWindow windowAgg = (LogicalWindow) rel;		
		WindowType windowType = (windowAgg.groups.get(0).isRows) ? WindowType.ROW_BASED : WindowType.RANGE_BASED;
		int windowRange = createWindowFrame(windowAgg.getConstants());		
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		window = new WindowDefinition (windowType, windowRange, windowSlide);		
		System.out.println("Window is : " + window.getWindowType().toString() + " with " + window.toString());
		AggregationUtil aggrHelper = new AggregationUtil();
		Pair<AggregationType [],FloatColumnReference []>  aggr = aggrHelper.getAggregationTypesAndAttributes(windowAgg.groups.get(0).getAggregateCalls(windowAgg));
		AggregationType [] aggregationTypes = aggr.left;
		FloatColumnReference [] aggregationAttributes = aggr.right;
		
		//Expression [] groupByAttributes = aggrHelper.getGroupByAttributes(windowAgg.groups.get(0).keys, schema);
		numberOfGroupByAttributes = windowAgg.groups.get(0).keys.length(); // groupByAttributes.length;
		// exclude rowtime, floor and ceil from group by attributes
		List<Integer> limitedGroupByList = new ArrayList<Integer>();
		for ( Integer groupby : windowAgg.groups.get(0).keys){
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
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);				
		
		outputSchema = ((Aggregation) cpuCode).getOutputSchema();		
		outputSchema = aggrHelper.createOutputSchema(aggregationTypes, aggregationAttributes, limitedGroupByAttributes, schema,outputSchema);	
		// fix the output or throw errors
		//outputSchema = aggrHelper.createOutputSchemaForWindow(aggregationTypes, aggregationAttributes, schema);			
	}

	private int createWindowFrame(List<RexLiteral> constants) {
		int windowFrame = 0;
		for ( RexLiteral con : constants) 
			windowFrame += Integer.parseInt(con.toString());
		// fix unbounded window range
		if (windowFrame == 0)
			windowFrame++;
		return windowFrame;
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

	@Override
	public Pair<Integer, Integer> getWindowOffset() {
		int windowOffset = this.schema.numberOfAttributes() - numberOfGroupByAttributes;
		return new Pair<Integer, Integer> (windowOffset,this.windowBarrier) ;
	}
	
}