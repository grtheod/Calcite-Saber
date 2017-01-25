package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.PredicateUtil;
import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.ThetaJoin;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ThetaJoinKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

public class SaberJoinRule implements SaberRule{
	
	public static final String usage = "usage: ThetaJoin";
	
	RelNode rel;
	WindowDefinition window1, window2;
	int [] offsets;
	ITupleSchema schema1,schema2;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	int queryId = 0;
	long timestampReference = 0;
	int batchSize;
	
	public SaberJoinRule(ITupleSchema schema1, ITupleSchema schema2, RelNode rel, int queryId, long timestampReference, WindowDefinition window1, WindowDefinition window2, int batchSize){
		this.rel = rel;
		this.schema1 = schema1;
		this.schema2 = schema2;
		this.window1 = window1;
		this.window2 = window2;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.batchSize = batchSize;
	}
	
	public void prepareRule() {

		if ((window1 == null) || ((window1.getSize()==1) && (window1.getSlide()==1))) {
			WindowType windowType1 = WindowType.ROW_BASED;
			int windowRange1 = 1024;
			int windowSlide1 = 1024;
			window1 = new WindowDefinition (windowType1, windowRange1, windowSlide1);
			
		}
		if ((window2 == null) || ((window2.getSize()==1) && (window2.getSlide()==1))) {
			WindowType windowType2 = WindowType.ROW_BASED;
			int windowRange2 = 1024;
			int windowSlide2 = 1024;
			window2 = new WindowDefinition (windowType2, windowRange2, windowSlide2);
		}
		System.out.println("Window1 is : " + window1.getWindowType().toString() + " with " + window1.toString());
		System.out.println("Window2 is : " + window2.getWindowType().toString() + " with " + window2.toString());
		LogicalJoin join = (LogicalJoin) rel;
		RexNode condition = join.getCondition();
		
		QueryConf queryConf = new QueryConf (batchSize);										
				
		PredicateUtil predicateHelper = new PredicateUtil();
		int joinOffset =  schema1.numberOfAttributes(); //use the joinOffset to fix the join condition.
		Pair<RexNode, IPredicate> pair = predicateHelper.getCondition(condition,joinOffset);
		IPredicate predicate = pair.right;
		System.out.println("Join Expr is : "+ predicate.toString());
		
		cpuCode = new ThetaJoin (schema1, schema2, predicate);
		gpuCode = new ThetaJoinKernel (schema1, schema2, predicate, null, batchSize, 1048576);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);		
		
		query = new Query (queryId, operators, schema1, window1, schema2, window2, queryConf, timestampReference);
		outputSchema = ExpressionsUtil.mergeTupleSchemas(schema1, schema2);
		//System.out.println("schema1" + schema1.getSchema()); System.out.println("schema2" + schema2.getSchema());
		int attributesOfSchema1 = schema1.numberOfAttributes();
		int attributesOfOutputSchema = outputSchema.numberOfAttributes();
		for (int i = 0; i < attributesOfSchema1; i++){
			outputSchema.setAttributeName(i, schema1.getAttributeName(i));
		}
		for (int i = attributesOfSchema1; i < attributesOfOutputSchema; i++){
			outputSchema.setAttributeName(i, schema2.getAttributeName(i - attributesOfSchema1));
		}
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

	public WindowDefinition getWindow() {
		return window1; //return different value
	}

	public WindowDefinition getWindow2() {
		return window2;
	}

	public Pair<Integer, Integer> getWindowOffset() {
		return new Pair<Integer, Integer>(0,0);
	}
}
