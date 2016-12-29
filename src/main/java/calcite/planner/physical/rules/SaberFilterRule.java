package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
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
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

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
	
	public SaberFilterRule(ITupleSchema schema, RelNode rel, int queryId , long timestampReference, WindowDefinition window){
		this.rel = rel;
		this.schema = schema;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.window = window;
	}
	
	public void prepareRule() {
	
		int batchSize = 1048576;
		WindowType windowType = (window!=null) ? window.getWindowType() : WindowType.ROW_BASED;
		long windowRange = (window!=null) ? window.getSize() : 1;
		long windowSlide = (window!=null) ? window.getSlide() : 1;
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
		return this.window;
	}

	public WindowDefinition getWindow2() {
		return null;
	}
	
	public int getWindowOffset() {
		return 0;
	}
	
}
