package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexLiteral;
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
	
	public SaberWindowRule(ITupleSchema schema,RelNode rel, int queryId, long timestampReference, WindowDefinition window){
		this.rel = rel;
		this.schema = schema;
		this.window = window;
		this.queryId = queryId;
		this.timestampReference = timestampReference;	
	}
	
	public void prepareRule() {
	
		int batchSize = 1048576;
		int windowSlide = 1;
		if (!(window == null))
			windowSlide = (int) window.getSlide(); 
		
		/*At this moment, a window has only one group of aggregate functions.*/
		LogicalWindow windowAgg = (LogicalWindow) rel;		
		WindowType windowType = (windowAgg.groups.get(0).isRows) ? WindowType.ROW_BASED : WindowType.RANGE_BASED;
		int windowRange = createWindowFrame(windowAgg.getConstants());		
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		window = new WindowDefinition (windowType, windowRange, windowSlide);		
		//System.out.println("window is : " + window.toString());
		AggregationUtil aggrHelper = new AggregationUtil();
		Pair<AggregationType [],FloatColumnReference []>  aggr = aggrHelper.getAggregationTypesAndAttributes(windowAgg.groups.get(0).getAggregateCalls(windowAgg));
		AggregationType [] aggregationTypes = aggr.left;
		FloatColumnReference [] aggregationAttributes = aggr.right;
		
		Expression [] groupByAttributes = aggrHelper.getGroupByAttributes(windowAgg.groups.get(0).keys, schema);
		
		cpuCode = new Aggregation (window, aggregationTypes, aggregationAttributes, groupByAttributes);
		System.out.println(cpuCode);
		if (groupByAttributes == null) 
			gpuCode = new ReductionKernel (window, aggregationTypes, aggregationAttributes, schema, batchSize);
		else
			gpuCode = new AggregationKernel (window, aggregationTypes, aggregationAttributes, groupByAttributes, schema, batchSize);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);				

		outputSchema = aggrHelper.createOutputSchemaForWindow(aggregationTypes, aggregationAttributes, schema);	
	}

	private int createWindowFrame(List<RexLiteral> constants) {
		int windowFrame = 0;
		for ( RexLiteral con : constants) 
			windowFrame += Integer.parseInt(con.toString());
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
	
}