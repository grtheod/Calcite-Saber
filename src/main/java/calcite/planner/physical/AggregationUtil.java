package calcite.planner.physical;

import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
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
				column = 0; //fix the column
			} else {				
				column = ((AggregateCall) aggs.get(i)).getArgList().get(0); 				
			}
			aggregationAttributes[i] = new FloatColumnReference(column);
			System.out.println("[DBG] aggregation Attribute string is " + aggregationAttributes[i]);
		}
		return new Pair<AggregationType[], FloatColumnReference[]>(aggregationTypes,aggregationAttributes);
	}

	/* Get the group by attributes*/
	public Expression[] getGroupByAttributes(ImmutableBitSet groupSet, ITupleSchema schema) {
		Expression [] groupByAttributes = null;
		if (!groupSet.isEmpty()){
			groupByAttributes = new Expression[groupSet.toList().size()];
			int numberOfGroups = 0;
			for (Integer ga : groupSet){
				if (schema.getAttributeType(ga).toString().equals("INT"))
					groupByAttributes[numberOfGroups] = new IntColumnReference(ga);
				else if (schema.getAttributeType(ga).toString().equals("FLOAT"))
					groupByAttributes[numberOfGroups] = new FloatColumnReference(ga);
				else if (schema.getAttributeType(ga).toString().equals("LONG"))
					groupByAttributes[numberOfGroups] = new LongColumnReference(ga);
				else {
					//throw exception
				}

				numberOfGroups++;
			}
			System.out.println("Number of groupByAttributes : "  + groupByAttributes.length);
		}
		return groupByAttributes;
	}	
	
	/* Create output schema */
	public ITupleSchema createOutputSchema(AggregationType[] aggregationTypes, FloatColumnReference[] aggregationAttributes, Expression[] groupByAttributes, 
			ITupleSchema schema, ITupleSchema outputSchema) {	
		int i = 0;
		int numberOfKeyAttributes = 0; 
		int n = outputSchema.numberOfAttributes();
		outputSchema.setAttributeName(0, "rowcount"); //by default the first column is related with LongColumnReference(0) and must be first
		if (groupByAttributes == null) {
			outputSchema.setAttributeName(n-1, "CNT()");		
			n = n - 1;
		} else 
			numberOfKeyAttributes = groupByAttributes.length;
			
		String name;
		if (numberOfKeyAttributes > 0) {			
			for (i = 0; i < numberOfKeyAttributes; ++i) {					
				name = schema.getAttributeName(Integer.parseInt(groupByAttributes[i].toString().replace("\"", "")));
				outputSchema.setAttributeName(i + 1, name);
			}
		}		
				 			
		numberOfKeyAttributes++; //fix the array pointer		
		//System.out.println("oS:"+outputSchema.getSchema()+" nOfKeyAttrs:"+numberOfKeyAttributes+" n:"+n);
		for (i = numberOfKeyAttributes; i < n ; ++i) {
		 	name = schema.getAttributeName(Integer.parseInt(aggregationAttributes[i - numberOfKeyAttributes].toString().replace("\"", "")));
			outputSchema.setAttributeName(i, aggregationTypes[i - numberOfKeyAttributes].toString() + "("
					+ name + ")");
		}		
		return outputSchema;
	}

	public ITupleSchema createOutputSchemaForWindow(AggregationType[] aggregationTypes,
			FloatColumnReference[] aggregationAttributes, ITupleSchema schema) {
		ITupleSchema outputSchema = null;
		int numberOfAttributes = schema.numberOfAttributes();
		int n = numberOfAttributes + aggregationAttributes.length;
		Expression [] outputAttributes = new Expression[n];
		
		int i;
		for (i=0; i < numberOfAttributes; i++) {
			if (schema.getAttributeType(i).toString().equals("INT"))
				outputAttributes[i] = new IntColumnReference(i);
			else if (schema.getAttributeType(i).toString().equals("FLOAT"))
				outputAttributes[i] = new FloatColumnReference(i);
			else if (schema.getAttributeType(i).toString().equals("LONG"))
				outputAttributes[i] = new LongColumnReference(i);				
		}
		for (i = numberOfAttributes; i < n; ++i) //fix the column references
			outputAttributes[i] = new FloatColumnReference(i);
										
		//set column names
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(outputAttributes);
		
		String name;
		if (numberOfAttributes > 0) {			
			for (i = 0; i < numberOfAttributes; ++i) {					
				name = schema.getAttributeName(i);
				outputSchema.setAttributeName(i, name);
			}
		}
		for (i = numberOfAttributes; i < n; ++i){
		 	name = schema.getAttributeName(Integer.parseInt(aggregationAttributes[i - numberOfAttributes].toString().replace("\"", "")));
			outputSchema.setAttributeName(i, aggregationTypes[i - numberOfAttributes].toString() + "("
					+ name + ")");
		}		
		return outputSchema;
	}
	
}
