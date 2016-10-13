package calcite.planner.physical.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.ThetaJoin;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ThetaJoinKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;

public class SaberJoinRule implements SaberRule{
	
	public static final String usage = "usage: ThetaJoin";
	
	public static final int      EQUAL_OP = 0;
	public static final int   NONEQUAL_OP = 1;
	public static final int       LESS_OP = 2;
	public static final int    NONLESS_OP = 3;
	public static final int    GREATER_OP = 4;
	public static final int NONGREATER_OP = 5;
	
	List<String> args = new ArrayList<>();
	int [] offsets;
	ITupleSchema schema1,schema2;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	
	
	public SaberJoinRule(ITupleSchema schema1, ITupleSchema schema2, List<String> args){
		this.args=args;
		this.schema1=schema1;
		this.schema2=schema2;
	}
	
	public Query ThetaJoinWithoutPredicated (){

		int batchSize = 1048576;
		WindowType windowType1 = WindowType.ROW_BASED;
		int windowRange1 = 1;
		int windowSlide1 = 1;
		int numberOfAttributes1 = 6;
		WindowType windowType2 = WindowType.ROW_BASED;
		int windowRange2 = 1;
		int windowSlide2 = 1;
		int numberOfAttributes2 = 6;
		int comparisons = 1;
		int queryId = 0;
		String operands = null;
		long timestampReference = 0;
		
		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.size(); ) {
			if ((j = i + 1) == args.size()) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args.get(i).equals("--batch-size")) { 
				batchSize = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--window-type-of-first-stream")) { 
				windowType1 = WindowType.fromString(args.get(j));
			} else
			if (args.get(i).equals("--window-range-of-first-stream")) { 
				windowRange1 = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--window-slide-of-first-stream")) { 
				windowSlide1 = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--input-attributes-of-first-stream")) { 
				numberOfAttributes1 = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--window-type-of-second-stream")) { 
				windowType2 = WindowType.fromString(args.get(j));
			} else
			if (args.get(i).equals("--window-range-of-second-stream")) { 
				windowRange2 = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--window-slide-of-second-stream")) { 
				windowSlide2 = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--input-attributes-of-second-stream")) { 
				numberOfAttributes2 = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--comparisons")) { 
				comparisons = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--operands")) {
				operands = args.get(j);
			} else
			if (args.get(i).equals("--queryId")) {
				queryId = Integer.parseInt(args.get(j));
			} else
			if (args.get(i).equals("--timestampReference")) {
				timestampReference = Long.parseLong(args.get(j));
			} 
			i = j + 1;
		}
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
				
		WindowDefinition window2 = new WindowDefinition (windowType2, windowRange2, windowSlide2);
				
		IPredicate predicate = getJoinCondition(operands);
		
		cpuCode = new ThetaJoin (schema1, schema2, predicate);
		gpuCode = new ThetaJoinKernel (schema1, schema2, predicate, null, batchSize, 1048576);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);		
		
		Query query = new Query (queryId, operators, schema1, window1, schema2, window2, queryConf, timestampReference);
		
		return query;
	}

	private IPredicate getJoinCondition(String operands) {
		IPredicate predicate = null;
		String condition = operands.substring(operands.indexOf("[")+1,operands.indexOf("]"));
		if ( !( (condition.contains("OR")) || (condition.contains("AND")))) {
			predicate = createSimpleJoinCondition(operands);
			System.out.println("Simple Join Expr : "+ predicate.toString());
		} else {
			predicate = createComplexJoinCondition(operands);
			System.out.println("Complex Join Expr : "+ predicate.toString());
		}

		return predicate;
	}


	private IPredicate createSimpleJoinCondition(String operands) {
		String compOp = operands.substring(0,operands.indexOf("("));
		int comparisonOperator = getComparisonOperator(compOp);
		String [] ops = operands.substring(operands.indexOf("(")+1).replace(")", "").split(",");
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

	private IPredicate createComplexJoinCondition(String operands) {
		/*
		IPredicate [] predicates = new IPredicate [comparisons];
		for (i = 0; i < comparisons; i++) {
			predicates[i] = new IntComparisonPredicate
					(IntComparisonPredicate.GREATER_OP, new IntColumnReference(1), new IntColumnReference(1));
		}
		IPredicate predicate = new ANDPredicate (predicates);
		 * */
		return null;
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
	
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}

	public Query getQuery() {
		return this.query;
	}

	public IOperatorCode getCpuCode() {
		return this.cpuCode;
	}

	public IOperatorCode getGpuCode() {
		return this.gpuCode;
	}
}
