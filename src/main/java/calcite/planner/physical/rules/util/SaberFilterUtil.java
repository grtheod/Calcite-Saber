package calcite.planner.physical.rules.util;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.PredicateUtil;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

public class SaberFilterUtil implements SaberRuleUtil{
	
	WindowDefinition window;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode filterCpuCode;
	IOperatorCode filterGpuCode;
	int batchSize;
	QueryOperator filterOperator;
	RexNode condition;
	
	public SaberFilterUtil(RexNode condition, int batchSize, ITupleSchema schema) {
		this.condition = condition;
		this.batchSize = batchSize;
		this.schema =schema;
	}

	public void build () {
		
		PredicateUtil predicateHelper = new PredicateUtil();
		Pair<RexNode, IPredicate> pair = predicateHelper.getCondition(condition, 0);
		IPredicate predicate = pair.right;
		System.out.println("Filter Expr is : "+ predicate.toString());
		filterCpuCode = new Selection (predicate);
		filterGpuCode = new SelectionKernel (schema, predicate, null, batchSize);
		this.filterOperator = new QueryOperator (filterCpuCode, filterGpuCode);
		this.outputSchema = schema;
		
	}
	
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}
	
	public IOperatorCode getCpuCode(){
		return this.filterCpuCode;
	}
	
	public IOperatorCode getGpuCode(){
		return this.filterGpuCode;
	}

	public WindowDefinition getWindow() {
		return this.window;
	}
	
	public QueryOperator getOperator() {
		return this.filterOperator;
	}

	@Override
	public WindowDefinition getWindow2() {
		return null;
	}
}
