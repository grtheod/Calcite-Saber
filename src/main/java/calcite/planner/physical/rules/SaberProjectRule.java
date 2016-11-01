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
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
/*Wrong result after join*/
public class SaberProjectRule implements SaberRule {
	public static final String usage = "usage: Projection";
	String args;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	int queryId = 0;
	long timestampReference = 0;
	
	public SaberProjectRule(ITupleSchema schema, String args, int queryId , long timestampReference){
		this.schema=schema;
		this.args=args;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
	}
	
	public void prepareRule() {
	
		int batchSize = 1048576;
		WindowType windowType = WindowType.ROW_BASED;
		int windowRange = 1;
		int windowSlide = 1;
		String operands = args;
		int projectedAttributes = 0;
		int expressionDepth = 1;
				
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window = new WindowDefinition (windowType, windowRange, windowSlide);
				
		List <String> projectedColumns = getProjectedColumns(operands);
		projectedAttributes = projectedColumns.size();
		
		Expression [] expressions = new Expression [projectedAttributes + 1];
		/* Always project the timestamp */
		expressions[0] = new LongColumnReference(0);
			
		int column;
		for (int i = 0; i < projectedAttributes; ++i){
			column = Integer.parseInt(projectedColumns.get(i));
			if (schema.getAttributeType(column + 1).equals(PrimitiveType.INT))
				expressions[i + 1] = new IntColumnReference (column + 1);
			else if (schema.getAttributeType(column + 1).equals(PrimitiveType.FLOAT)) 
				expressions[i + 1] = new FloatColumnReference (column + 1);
			else
				expressions[i + 1] = new LongColumnReference (column + 1);
		}
		
		/*Creating output Schema*/
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
		for (int i = 0; i < projectedAttributes; ++i){		
			column = Integer.parseInt(projectedColumns.get(i));
			outputSchema.setAttributeName(i+1, schema.getAttributeName(column + 1));
		}
				
		IOperatorCode cpuCode = new Projection (expressions);
		IOperatorCode gpuCode = new ProjectionKernel (schema, expressions, batchSize, expressionDepth);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
			
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);				
	}
	
	/* A method to get the columns that we will project (without expressions)*/
	public List <String> getProjectedColumns(String operands){
		List <String> prColumns = new ArrayList<String>();		
		String [] operand = operands.split(","); 
		for( String op : operand){
			op = op.replace("[", "").replace("]", "").replace("$", "").trim();			
			prColumns.add(op);
		}				
		return prColumns;
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
