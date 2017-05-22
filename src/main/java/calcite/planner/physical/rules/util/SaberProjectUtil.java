package calcite.planner.physical.rules.util;

import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.ExpressionBuilder;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;

public class SaberProjectUtil implements SaberRuleUtil{
	
	WindowDefinition window;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode projectCpuCode;
	IOperatorCode projectGpuCode;
	int batchSize;
	QueryOperator projectOperator;
	List<RexNode> projectedAttrs;
	int windowOffset;
	int windowBarrier;
	boolean validProject;

	public SaberProjectUtil (List<RexNode> projectedAttrs, int batchSize, ITupleSchema schema, WindowDefinition window, int windowOffset, int windowBarrier) {
		this.projectedAttrs = projectedAttrs;
		this.batchSize = batchSize;
		this.schema = schema;
		this.window = window;
		this.windowOffset = windowOffset;
		this.windowBarrier = windowBarrier;
	}
	
	public void build() {
		
		WindowType windowType = (window!=null) ? window.getWindowType() : WindowType.ROW_BASED;
		long windowRange = (window!=null) ? window.getSize() : 1;
		long windowSlide = (window!=null) ? window.getSlide() : 1;
		
		this.outputSchema = this.schema;
		validProject = false;
		
		int numberOfInputAttrs = schema.numberOfAttributes();
		int projectedAttributes = projectedAttrs.size();
		int expressionDepth = 1;
		
		Expression [] expressions = new Expression [projectedAttributes];
		/* Always project the timestamp */
		//expressions[0] = new LongColumnReference(0);

		int column;
		int i = 0;
		for (RexNode attr : projectedAttrs){
			//System.out.println(attr.toString());
			if (attr.getKind().toString().equals("INPUT_REF")) {				
				column = Integer.parseInt(attr.toString().replace("$", ""));
				if ((windowBarrier >= 0) && (column >= windowOffset) && (column > 0)){ //fix the offset when the previous operator was LogicalWindow
					//if (column==windowOffset)column+=1; 
					column -= windowBarrier;	
				}				
				if ((windowBarrier < 0) && (column >= windowOffset) && (windowOffset > 0)) //fix the offset when the previous operator was LogicalAggregate
					column -= 1;
				
				if (column < 0 && windowBarrier > 0) column+= windowBarrier;
				if (column >= numberOfInputAttrs) column -= 1;
				
				if (schema.getAttributeType(column).equals(PrimitiveType.INT))
					expressions[i] = new IntColumnReference (column);
				else if (schema.getAttributeType(column).equals(PrimitiveType.FLOAT)) 
					expressions[i] = new FloatColumnReference (column);
				else
					expressions[i] = new LongColumnReference (column);
			} else { 
				//pass the windowOffset to more complex expressions
				Pair<Expression, Pair<Integer, Integer>> pair = new ExpressionBuilder(attr, windowOffset).build();			
				expressions[i] = pair.left;
				if (pair.right.left> 0) {
					windowRange = pair.right.left;
					windowSlide = pair.right.right;
					windowType = WindowType.RANGE_BASED;
				}
			}			
			i++;
		}
		
		/*Creating output Schema*/
		ITupleSchema newOutputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
		i = 0;
		for (RexNode attr : projectedAttrs){
			if (attr.getKind().toString().equals("INPUT_REF")) {
				column = Integer.parseInt(attr.toString().replace("$", ""));
				if ((windowBarrier >= 0) && (column >= windowOffset) && (column > 0)){ //fix the offset when the previous operator was LogicalWindow
					//if (column==windowOffset)column+=1; 
					column -= windowBarrier;	
				}				
				if ((windowBarrier < 0) && (column >= windowOffset) && (windowOffset > 0)) //fix the offset when the previous operator was LogicalAggregate
					column -= 1;
				
				if (column < 0 && windowBarrier > 0) column+= windowBarrier;
				if (column >= numberOfInputAttrs) column -= 1;
				
				newOutputSchema.setAttributeName(i, schema.getAttributeName(column));
			} else {
				newOutputSchema.setAttributeName(i, attr.toString());
			}			
			i++;
		}
		
		
		// check if it works in all cases!!!
		// Remove redundant projections, especially after aggregate.
		if (outputSchema==null || !newOutputSchema.getSchema().equals(outputSchema.getSchema())) {

			validProject = true;
			projectCpuCode = new Projection (expressions);
			projectGpuCode = new ProjectionKernel (schema, expressions, batchSize, expressionDepth);
			
			this.projectOperator = new QueryOperator (projectCpuCode, projectGpuCode);
			outputSchema = newOutputSchema;
			
			System.out.println("OutputSchema : " + outputSchema.getSchema());
		}
		
		window = new WindowDefinition (windowType, windowRange, windowSlide);
	}
	
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}
	
	public IOperatorCode getCpuCode(){
		return this.projectCpuCode;
	}
	
	public IOperatorCode getGpuCode(){
		return this.projectGpuCode;
	}

	public WindowDefinition getWindow() {
		return this.window;
	}
	
	public QueryOperator getOperator() {
		return this.projectOperator;
	}
	
	public boolean isValid() {
		return validProject;
	}

	@Override
	public WindowDefinition getWindow2() {
		return null;
	}
}
