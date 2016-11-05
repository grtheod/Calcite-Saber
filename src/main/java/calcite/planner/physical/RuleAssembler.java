package calcite.planner.physical;

import org.apache.calcite.rel.RelNode;

import calcite.planner.physical.rules.SaberAggregateRule;
import calcite.planner.physical.rules.SaberFilterRule;
import calcite.planner.physical.rules.SaberJoinRule;
import calcite.planner.physical.rules.SaberProjectRule;
import calcite.planner.physical.rules.SaberScanRule;
import calcite.planner.physical.rules.SaberWindowRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;

public class RuleAssembler {
	
	protected static final String PROJECT= "LogicalProject";
	protected static final String FILTER = "LogicalFilter";	
	protected static final String JOIN = "LogicalJoin";
	protected static final String AGGREGATE = "LogicalAggregate";
	protected static final String SCAN = "LogicalTableScan";
	protected static final String WINDOW = "LogicalWindow";
	
	String operator;
	RelNode rel;
	ITupleSchema schema1, schema2;
	int queryId;
	long timestampReference;
	
	public RuleAssembler(String operator, RelNode rel, ITupleSchema schema, int queryId, long timestampReference){
		this.operator = operator;
		this.rel = rel;
		this.schema1 = schema;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
	}
	
	public RuleAssembler(String operator, RelNode rel, ITupleSchema schema1, ITupleSchema schema2, int queryId, long timestampReference){
		this.operator = operator;
		this.rel = rel;
		this.schema1 = schema1;
		this.schema2 = schema2;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
	}
	
	public SaberRule construct(){
		
		switch (operator){			
			case PROJECT :			
				System.out.println("==> Assembling Projection");
				SaberProjectRule project = new SaberProjectRule(schema1, rel, queryId, timestampReference);
				project.prepareRule();				
				return project;
			case FILTER :
				System.out.println("==> Assembling Filter");
				SaberFilterRule filter = new SaberFilterRule(schema1, rel, queryId, timestampReference);
				filter.prepareRule();				
				return filter;			
			case JOIN :
				System.out.println("==> Assembling Join");
				SaberJoinRule join = new SaberJoinRule(schema1, schema2, rel, queryId, timestampReference);
				join.prepareRule();				
				return join;							
			case AGGREGATE : 				
				System.out.println("==> Assembling Aggregate");
				SaberAggregateRule aggregate = new SaberAggregateRule(schema1, rel, queryId, timestampReference);
				aggregate.prepareRule();				
				return aggregate;			
			case SCAN :
				System.out.println("==> Assembling Scan");
				SaberScanRule scan = new SaberScanRule(schema1);
				scan.prepareRule();				
				return scan;
			case WINDOW :
				System.out.println("==> Assembling Window");
				SaberWindowRule window = new SaberWindowRule(schema1, rel, queryId, timestampReference);
				window.prepareRule();				
				return window;
			default :
				System.err.println("error: invalid rule");
				System.exit(1);
				return null;
		}		
	}
	

}
