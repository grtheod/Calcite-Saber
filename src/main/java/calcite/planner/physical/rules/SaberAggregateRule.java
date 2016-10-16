package calcite.planner.physical.rules;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.util.Pair;

import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatDivision;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatMultiplication;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ReductionKernel;

public class SaberAggregateRule implements SaberRule{
	public static final String usage = "usage: Aggregate";
	String args;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	int queryId = 0;
	long timestampReference = 0;
	
	
	public SaberAggregateRule(ITupleSchema schema,String args, int queryId, long timestampReference){
		this.args = args;
		this.schema = schema;
		this.queryId = queryId;
		this.timestampReference = timestampReference;	
	}
	
	public void prepareRule() {
	
		int batchSize = 1048576;
		WindowType windowType = WindowType.ROW_BASED;
		int windowRange = 1024;
		int windowSlide = 1024;
		String operands = args;
		int i;
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window = new WindowDefinition (windowType, windowRange, windowSlide);		
		
		Pair<AggregationType [],FloatColumnReference []>  aggr = getAggregationTypesAndAttributes(operands);
		AggregationType [] aggregationTypes = aggr.left;
		FloatColumnReference [] aggregationAttributes = aggr.right;
		
		Expression [] groupByAttributes = getGroupByAttributes(operands);
		
		cpuCode = new Aggregation (window, aggregationTypes, aggregationAttributes, groupByAttributes);
		System.out.println(cpuCode);
		if (groupByAttributes != null) /*check if it works*/
			gpuCode = new ReductionKernel (window, aggregationTypes, aggregationAttributes, schema, batchSize);
		else
			gpuCode = new AggregationKernel (window, aggregationTypes, aggregationAttributes, groupByAttributes, schema, batchSize);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);				
		
		/* Create output schema */	
		int numberOfKeyAttributes = groupByAttributes.length;
		int n = numberOfKeyAttributes + aggregationTypes.length + 2; // add one column for timestamp and one for count
		Expression [] outputAttributes = new Expression[n]; 		
		outputAttributes[0] = new LongColumnReference(0);

		if (numberOfKeyAttributes > 0) {			
			for (i = 1; i <= numberOfKeyAttributes; ++i) {				
				Expression e = groupByAttributes[i - 1];
				     if (e instanceof   IntExpression) { outputAttributes[i] = new   IntColumnReference(i);}
				else if (e instanceof  LongExpression) { outputAttributes[i] = new  LongColumnReference(i);}
				else if (e instanceof FloatExpression) { outputAttributes[i] = new FloatColumnReference(i);}
				else
					throw new IllegalArgumentException("error: invalid group-by attribute");
			}
		}
		
		for (i = numberOfKeyAttributes + 1; i < n; ++i)
			outputAttributes[i] = new FloatColumnReference(i);
		
		/* Set count attribute */
		if (groupByAttributes == null)
			outputAttributes[n - 1] = new IntColumnReference(n - 1);					
		
		//set column names
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		String name;
		if (numberOfKeyAttributes > 0) {			
			for (i = 1; i <= numberOfKeyAttributes; ++i) {					
				name = schema.getAttributeName(Integer.parseInt(groupByAttributes[i-1].toString().replace("\"", "")));
				outputSchema.setAttributeName(i, name);
			}
		}
		for (i = numberOfKeyAttributes + 1; i < n - 1; ++i){
		 	name = schema.getAttributeName(Integer.parseInt(aggregationAttributes[i - numberOfKeyAttributes - 1].toString().replace("\"", "")));
			outputSchema.setAttributeName(i, aggregationTypes[i - numberOfKeyAttributes - 1].toString() + "("
					+ name + ")");
		}
		
		/* Set count attribute */
		outputSchema.setAttributeName(i,   "CNT(*"+ ")");
		
	}

	/* Get the aggregations and their references to columns. Count is  assigned to timestamp column.*/
	private Pair<AggregationType[], FloatColumnReference[]> getAggregationTypesAndAttributes(String operands) {
		String aggr [] = operands.substring(operands.indexOf(",")+1).split(",");
		
		AggregationType [] aggregationTypes = new AggregationType [aggr.length];
		String aggregate = null;
		for (int i = 0; i < aggr.length; ++i) {
			aggregate = aggr[i].substring(aggr[i].indexOf("=")+1, aggr[i].indexOf("("));
			if (aggregate.equals("COUNT")){ aggregate="CNT";}
			System.out.println("[DBG] aggregation type string is " + aggregate);
			aggregationTypes[i] = AggregationType.fromString(aggregate);
						
		}
		
		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [aggr.length];
		int column;
		String exp;
		for (int i = 0; i < aggr.length; ++i){
			if (aggregationTypes[i] == AggregationType.CNT) {
				column = 0;
			} else {
				exp = aggr[i].substring(aggr[i].indexOf("(")+2, aggr[i].indexOf(")"));
				column = Integer.parseInt(exp.replace(")","")) + 1;
			}
			aggregationAttributes[i] = new FloatColumnReference(column);
			System.out.println("[DBG] aggregation Attribute string is " + aggregationAttributes[i]);
		}
		return new Pair<AggregationType[], FloatColumnReference[]>(aggregationTypes,aggregationAttributes);
	}

	/* Get the group by attributes*/
	private Expression[] getGroupByAttributes(String operands) {
		Expression [] groupByAttributes = null;
		String groupAttrs = operands.substring(operands.indexOf("{")+1, operands.indexOf("}")).trim();

		if (!(groupAttrs.equals(" ")) && !(groupAttrs.equals(""))){
			String gAttrs [] = groupAttrs.split(",");
			groupByAttributes = new Expression[gAttrs.length];
			int numberOfGroups = 0;
			for (String ga : gAttrs){
				groupByAttributes[numberOfGroups] = new IntColumnReference(Integer.parseInt(ga.trim()) +1);
				numberOfGroups++;
			}
			System.out.println("Number of groupByAttributes : "  + groupByAttributes.length);
		}
		return groupByAttributes;
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
	
}
