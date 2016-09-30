package calcite.planner.physical.rules;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.ThetaJoin;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ThetaJoinKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;

public class SaberJoinRule {
	
	public static final String usage = "usage: ThetaJoin";
	List<String> args = new ArrayList<>();
	int [] offsets;
	ITupleSchema schema1,schema2;
	ITupleSchema outputSchema;
	
	SaberJoinRule(ITupleSchema schema1, ITupleSchema schema2, List<String> args){
		this.args=args;
		this.schema1=schema1;
		this.schema2=schema2;
	}
	
	public Query ThetaJoinWithoutPredicated (){
		String executionMode = "cpu";
		int numberOfThreads = 1;
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
		int tuplesPerInsert = 128;
		
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
			if (args.get(i).equals("--tuples-per-insert")) { 
				tuplesPerInsert = Integer.parseInt(args.get(j));
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args.get(i), args.get(j)));
				System.exit(1);
			}
			i = j + 1;
		}
		
		SystemConf.CPU = false;
		SystemConf.GPU = false;
		
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		
		SystemConf.THREADS = numberOfThreads;
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		WindowDefinition window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
		
		/* Reset tuple size */
		int tupleSize1 = schema1.getTupleSize();
		
		WindowDefinition window2 = new WindowDefinition (windowType2, windowRange2, windowSlide2);
		
		/* Reset tuple size */
		int tupleSize2 = schema2.getTupleSize();
		
		IPredicate [] predicates = new IPredicate [comparisons];
		for (i = 0; i < comparisons; i++) {
			predicates[i] = new IntComparisonPredicate
					(IntComparisonPredicate.GREATER_OP, new IntColumnReference(1), new IntColumnReference(1));
		}
		IPredicate predicate = new ANDPredicate (predicates);
		
		IOperatorCode cpuCode = new ThetaJoin (schema1, schema2, predicate);
		IOperatorCode gpuCode = new ThetaJoinKernel (schema1, schema2, predicate, null, batchSize, 1048576);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		long timestampReference = System.nanoTime();
		
		Query query = new Query (0, operators, schema1, window1, schema2, window2, queryConf, timestampReference);
		
		return query;
	}
}
