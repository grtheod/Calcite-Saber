package calcite.planner.physical;

import java.util.List;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongExpression;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;

public class AggregationUtil {


	/* Get the aggregations and their references to columns. Count is  assigned to timestamp column.*/
	public Pair<AggregationType[], FloatColumnReference[]> getAggregationTypesAndAttributes(List<AggregateCall> aggs) {
		
		AggregationType [] aggregationTypes = new AggregationType [aggs.size()];
		String aggregate = null;
		for (int i = 0; i < aggs.size(); ++i) {
			//if (aggs.getClass().getName().equals(Aggregate.class.getName()))  
			aggregate = aggs.get(i).getAggregation().getKind().toString();			 				
			if (aggregate.equals("COUNT")){ aggregate="CNT";}
			if (aggregate.equals("SUM0")){ aggregate="SUM";}
			System.out.println("[DBG] aggregation type string is " + aggregate);
			aggregationTypes[i] = AggregationType.fromString(aggregate);						
		}
		
		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [aggs.size()];
		int column;
		for (int i = 0; i < aggs.size(); ++i){
			if (aggregationTypes[i] == AggregationType.CNT) {
				column = 0;
			} else {				
				column = ((AggregateCall) aggs.get(i)).getArgList().get(0) + 1; 				
			}
			aggregationAttributes[i] = new FloatColumnReference(column);
			System.out.println("[DBG] aggregation Attribute string is " + aggregationAttributes[i]);
		}
		return new Pair<AggregationType[], FloatColumnReference[]>(aggregationTypes,aggregationAttributes);
	}

	/* Get the group by attributes*/
	public Expression[] getGroupByAttributes(ImmutableBitSet groupSet) {
		Expression [] groupByAttributes = null;
		if (!groupSet.isEmpty()){
			groupByAttributes = new Expression[groupSet.toList().size()];
			int numberOfGroups = 0;
			for (Integer ga : groupSet){
				groupByAttributes[numberOfGroups] = new IntColumnReference(ga +1);
				numberOfGroups++;
			}
			System.out.println("Number of groupByAttributes : "  + groupByAttributes.length);
		}
		return groupByAttributes;
	}	
	
	/* Create output schema */
	public ITupleSchema createOutputSchema(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes, Expression[] groupByAttributes, ITupleSchema schema) {	
		ITupleSchema outputSchema = null;
		int i;
		int numberOfKeyAttributes = (groupByAttributes == null)? 0 : groupByAttributes.length;
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
		return outputSchema;
	}
	
}
