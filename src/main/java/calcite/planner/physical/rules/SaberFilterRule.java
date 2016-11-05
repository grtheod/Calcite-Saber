package calcite.planner.physical.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import calcite.planner.physical.PredicateUtil;
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
	
	public SaberFilterRule(ITupleSchema schema, RelNode rel, int queryId , long timestampReference){
		this.rel = rel;
		this.schema = schema;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
	}
	
	public void prepareRule() {
	
		int batchSize = 1048576;
		WindowType windowType = WindowType.ROW_BASED;
		int windowRange = 1;
		int windowSlide = 1;
		LogicalFilter filter = (LogicalFilter) rel;
		RexNode condition = filter.getCondition();
		
		QueryConf queryConf = new QueryConf (batchSize);
		
		window = new WindowDefinition (windowType, windowRange, windowSlide);
		
		PredicateUtil predicateHelper = new PredicateUtil();
		Pair<RexNode, IPredicate> pair = predicateHelper.getCondition(condition, 0);
		IPredicate predicate = pair.right;
		System.out.println("Filter Expr is : "+ predicate.toString());
		
		cpuCode = new Selection (predicate);
		gpuCode = new SelectionKernel (schema, predicate, null, batchSize);
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
		
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);		
		outputSchema = schema;			
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

	public WindowDefinition getWindow() {
		return window;
	}

	public WindowDefinition getWindow2() {
		return null;
	}
	
}
