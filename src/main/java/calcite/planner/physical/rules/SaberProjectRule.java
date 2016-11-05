package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
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
/*Wrong result after join*/
public class SaberProjectRule implements SaberRule {
	public static final String usage = "usage: Projection";
	
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
	
	public SaberProjectRule(ITupleSchema schema, RelNode rel, int queryId , long timestampReference){
		this.schema = schema;
		this.rel = rel;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
	}
	
	public void prepareRule() {
	
		int batchSize = 1048576;
		WindowType windowType = WindowType.ROW_BASED;
		int windowRange = 1;
		int windowSlide = 1;
		int projectedAttributes = 0;
		int expressionDepth = 1;
				
		QueryConf queryConf = new QueryConf (batchSize);
		
		window = new WindowDefinition (windowType, windowRange, windowSlide);
		
		List<RexNode> projectedAttrs = rel.getChildExps(); 
		
		projectedAttributes = projectedAttrs.size();
		
		Expression [] expressions = new Expression [projectedAttributes + 1];
		/* Always project the timestamp */
		expressions[0] = new LongColumnReference(0);
			
		int column;
		int i = 1;
		/*Fix the expressions.*/
		for (RexNode attr : projectedAttrs){
			if (attr.getKind().toString().equals("CASE")) //fix the case expression
				column =  Integer.parseInt(((RexCall)((RexCall) attr).operands.get(1)).getOperands().get(0).toString().replace("$", ""));
			else 
				column = Integer.parseInt(attr.toString().replace("$", "")) + 1;
			
			if (schema.getAttributeType(column).equals(PrimitiveType.INT))
				expressions[i] = new IntColumnReference (column);
			else if (schema.getAttributeType(column).equals(PrimitiveType.FLOAT)) 
				expressions[i] = new FloatColumnReference (column);
			else
				expressions[i] = new LongColumnReference (column);
			i++;
		}

		/*Creating output Schema*/
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
		i = 1;
		for (RexNode attr : projectedAttrs){
			if (attr.getKind().toString().equals("CASE"))
				column =  Integer.parseInt(((RexCall)((RexCall) attr).operands.get(1)).getOperands().get(0).toString().replace("$", ""));
			else 
				column = Integer.parseInt(attr.toString().replace("$", "")) + 1;
			outputSchema.setAttributeName(i, schema.getAttributeName(column));
			i++;
		}
		
		IOperatorCode cpuCode = new Projection (expressions);
		IOperatorCode gpuCode = new ProjectionKernel (schema, expressions, batchSize, expressionDepth);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
			
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
	
}
