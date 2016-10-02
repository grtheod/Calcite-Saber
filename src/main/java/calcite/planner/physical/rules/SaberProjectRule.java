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

public class SaberProjectRule implements SaberRule {
	public static final String usage = "usage: Projection";
	List<String> args = new ArrayList<>();
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	
	public SaberProjectRule(ITupleSchema schema,List<String> args){
		this.schema=schema;
		this.args=args;
	}
	
	public void prepareRule() {
	
		String executionMode = "cpu";
		int numberOfThreads = 15;
		int batchSize = 1048576;
		WindowType windowType = WindowType.ROW_BASED;
		int windowRange = 1;
		int windowSlide = 1;
		int numberOfAttributes = 6;
		int projectedAttributes = 1;
		int expressionDepth = 1;
		int tuplesPerInsert = 32768;
		String operands = null;
		String stringSchema = null;
		String table = null;
		int queryId = 0;
		long timestampReference = 0;
		
		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.size(); ) {
			if ((j = i + 1) == args.size()) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args.get(i).equals("--mode")) { 
				executionMode = args.get(j);
			} else
			if (args.get(i).equals("--threads")) {
				numberOfThreads = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--batch-size")) { 
				batchSize = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--window-type")) { 
				windowType = WindowType.fromString(args.get(j));
			} else
			if (args.get(i).equals("--window-range")) { 
				windowRange = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--window-slide")) { 
				windowSlide = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--input-attributes")) { 
				numberOfAttributes = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--projected-attributes")) { 
				projectedAttributes = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--depth")) { 
				expressionDepth = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--tuples-per-insert")) { 
				tuplesPerInsert = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--operands")) {
				operands = args.get(j);
			} else
			if (args.get(i).equals("--schema")) {
				stringSchema = args.get(j);
			} else
			if (args.get(i).equals("--table")) {
				table = args.get(j);
			} else
			if (args.get(i).equals("--queryId")) {
				queryId = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--timestampReference")) {
				timestampReference = Long.parseLong(args.get(j));
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args.get(i), args.get(j)));
				System.exit(1);
			}
			i = j + 1;
		}
		
		SystemConf.CIRCULAR_BUFFER_SIZE = 32 * 1048576;
		SystemConf.LATENCY_ON = false;
		
		SystemConf.CPU = false;
		SystemConf.GPU = false;
		
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		
		SystemConf.THREADS = numberOfThreads;
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window = new WindowDefinition (windowType, windowRange, windowSlide);
		
		/* Reset tuple size */
		int tupleSize = schema.getTupleSize();
		
		List <Pair<String,String>> projectedColumns = getProjectedColumns(operands);
		projectedAttributes = projectedColumns.size();
		
		Expression [] expressions = new Expression [projectedAttributes + 1];
		/* Always project the timestamp */
		expressions[0] = new LongColumnReference(0);
			
		int column;
		for (i = 0; i < projectedAttributes; ++i){
			column = Integer.parseInt(projectedColumns.get(i).right);
			expressions[i + 1] = new IntColumnReference (column + 1);
		}
		
		/*Creating output Schema*/
		outputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
		for (i = 0; i < projectedAttributes; ++i){			
			outputSchema.setAttributeName(i+1, projectedColumns.get(i).left);
		}
		
		/* Introduce 0 or more floating-point arithmetic expressions */
		/*FloatExpression f = new FloatColumnReference(1);
		for (i = 0; i < expressionDepth; i++)
			f = new FloatDivision (new FloatMultiplication (new FloatConstant(3), f), new FloatConstant(2));
		expressions[1] = f;
		*/
		
		IOperatorCode cpuCode = new Projection (expressions);
		IOperatorCode gpuCode = new ProjectionKernel (schema, expressions, batchSize, expressionDepth);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
				
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);		
		
	}
	
	/* A method to get the columns that we will project (without expressions)*/
	public List <Pair<String,String>> getProjectedColumns(String operands){
		List <Pair<String,String>> prColumns = new ArrayList<Pair<String,String>>();		
		String [] operand = operands.split(","); 
		for( String op : operand){
			op = op.replace("(", "").replace(")", "").replace("[$", "").replace("]", "").replace("]", "").trim();
			String [] parts = op.split("=");
			prColumns.add(new Pair<String,String>(parts[0],parts[1]));
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
