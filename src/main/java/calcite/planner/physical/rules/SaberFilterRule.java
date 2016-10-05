package calcite.planner.physical.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.util.Pair;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.ORPredicate;

public class SaberFilterRule implements SaberRule {
	
	public static final String usage = "usage: Filter";
	
	public static final int      EQUAL_OP = 0;
	public static final int   NONEQUAL_OP = 1;
	public static final int       LESS_OP = 2;
	public static final int    NONLESS_OP = 3;
	public static final int    GREATER_OP = 4;
	public static final int NONGREATER_OP = 5;
	
	List<String> args = new ArrayList<>();
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	
	public SaberFilterRule(ITupleSchema schema,List<String> args){
		this.args=args;
		this.schema=schema;
	}
	
	public void prepareRule() {
	
		String executionMode = "cpu";
		int numberOfThreads = 1;
		int batchSize = 1048576;
		WindowType windowType = WindowType.ROW_BASED;
		int windowRange = 1;
		int windowSlide = 1;
		int numberOfAttributes = 6;
		int comparisons = 1;
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
			if (args.get(i).equals("--comparisons")) { 
				comparisons = Integer.parseInt(args.get(j));
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
			} else
			if (args.get(i).equals("--whitespaces")) {
					
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args.get(i), args.get(j)));
				System.exit(1);
			}
			i = j + 1;
		}
		
		SystemConf.CIRCULAR_BUFFER_SIZE = 32 * 1048576; //maybe change the size??
		SystemConf.LATENCY_ON = false;
		
		SystemConf.PARTIAL_WINDOWS = 0;
		
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

		IPredicate predicate =  getFilterCondition(operands);
		
		cpuCode = new Selection (predicate);
		gpuCode = new SelectionKernel (schema, predicate, null, batchSize);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);		
		outputSchema = schema;		
		
	}

	/*
	 * Create filter conditions from a given String (only for integer Expressions).
	 * */
	public IPredicate getFilterCondition(String operands){
		IPredicate predicate = null;		
		operands =  operands.substring(operands.indexOf("=")+1); 
		//op = op.replace("(", "").replace(")", "").replace("[$", "").replace("]", "").replace("]", "").trim();
		if (!(operands.contains("AND")) && !(operands.contains("OR"))){
			predicate = createSimpleExpression(operands);
			System.out.println("Simple Expr : "+ predicate.toString());
		} else {
			predicate = createComplexExpression(operands);
			System.out.println("Complex Expr : "+ predicate.toString());
		}
				
		return predicate;
	}
	
	/* Match a given comparison operator to Saber's operator codes*/
	private int getComparisonOperator(String compOp) {
		int operatorCode; 
		switch (compOp){			
			case "<>" :
				operatorCode = NONEQUAL_OP; break;
			case "<" :
				operatorCode = LESS_OP; break;
			case ">=" :
				operatorCode = NONLESS_OP; break;
			case ">" :
				operatorCode = GREATER_OP; break;
			case "<=" :
				operatorCode = NONGREATER_OP; break;
			default:
				operatorCode = EQUAL_OP; //EQUAL_OP is considered default
		}
		return operatorCode;
	}	
	
	/* A method to create a simple expression with one comparison operator */
	public IPredicate createSimpleExpression(String operands){
		String compOp = operands.substring(0, operands.indexOf("(")).replace("[", "");
		int comparisonOperator = getComparisonOperator(compOp);
		String [] ops = operands.substring(operands.indexOf("(")+1).replace(")","").replace("]", "").split(",");		
		IntExpression firstOp,secondOp;
		
		if(ops[0].contains("$")){
			firstOp = new IntColumnReference(Integer.parseInt(ops[0].replace("$", "").trim()) + 1);
		}else {
			firstOp = new IntConstant(Integer.parseInt(ops[0].trim()));
		}
		if(ops[1].contains("$")){
			secondOp = new IntColumnReference(Integer.parseInt(ops[1].replace("$", "").trim()) + 1);
		}else {
			secondOp = new IntConstant(Integer.parseInt(ops[1].trim()));
		}
		
		return new IntComparisonPredicate(comparisonOperator, firstOp, secondOp);			
	}
	
	/* A method to create a complex expression that contains only ANDs/ORs*/
	public IPredicate createComplexExpression(String operands){
		boolean isAnd = true;
		if (operands.contains("OR")) {
			isAnd = false;
		}
		
		operands = operands.replace("OR", "").replace("AND", "").replace("[", "").replace("]", "");
		System.out.println(operands);
		Iterable<String> ops = Splitter.on("),").split(operands);
		int comparisons = Iterables.size(ops);
		IPredicate [] predicates = new IPredicate [comparisons];
		int i = 0;
		for ( String op : ops) {
			op = op.substring(1);
			System.out.println(op);
			predicates[i] = createSimpleExpression(op);
			i++;
		}
		
		if (isAnd) {
			return new ANDPredicate (predicates);
		}else {
			return new ORPredicate (predicates);
		}
	}

	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}
	
	public Query getQuery() {
		return this.query;
	}
	
	public IOperatorCode getCpuCode(){
		return this.cpuCode;
	}
	
	public IOperatorCode getGpuCode(){
		return this.gpuCode;
	}
	
}
