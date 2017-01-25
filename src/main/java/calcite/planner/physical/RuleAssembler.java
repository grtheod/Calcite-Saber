package calcite.planner.physical;

import org.apache.calcite.rel.RelNode;

import calcite.planner.physical.rules.SaberAggregateRule;
import calcite.planner.physical.rules.SaberFilterRule;
import calcite.planner.physical.rules.SaberJoinRule;
import calcite.planner.physical.rules.SaberProjectRule;
import calcite.planner.physical.rules.SaberScanRule;
import calcite.planner.physical.rules.SaberWindowRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowDefinition;

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
	WindowDefinition window1, window2;
	int queryId, windowOffset, windowBarrier, batchSize;
	long timestampReference;
	boolean flag = false;
	
	// constructor for table scan operator
	public RuleAssembler(String operator, ITupleSchema schema, int queryId, long timestampReference, boolean flag, int batchSize){
		this.operator = operator;
		this.schema1 = schema;
		this.queryId = queryId;		
		this.timestampReference = timestampReference;
		this.flag = flag;
		this.batchSize = batchSize;
	}
	
	// constructor for join operator
	public RuleAssembler(String operator, RelNode rel, ITupleSchema schema1, ITupleSchema schema2, int queryId, long timestampReference, WindowDefinition window1, WindowDefinition window2, int batchSize){
		this.operator = operator;
		this.rel = rel;
		this.schema1 = schema1;
		this.schema2 = schema2;
		this.window1 = window1;
		this.window2 = window2;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.batchSize = batchSize;
	}
	
	// constructor for the rest of operators
	public RuleAssembler(String operator, RelNode rel, ITupleSchema schema, int queryId, long timestampReference, WindowDefinition window, int windowOffset, int windowBarrier, int batchSize){
		this.operator = operator;
		this.rel = rel;
		this.schema1 = schema;
		this.window1 = window;
		this.queryId = queryId;		
		this.timestampReference = timestampReference;
		this.windowOffset = windowOffset;
		this.windowBarrier =  windowBarrier;
		this.batchSize = batchSize;
	}
	
	public SaberRule construct(){
		
		switch (operator){			
			case PROJECT :			
				System.out.println("==> Assembling Projection");
				SaberProjectRule project = new SaberProjectRule(schema1, rel, queryId, timestampReference, window1, windowOffset, windowBarrier, batchSize);
				project.prepareRule();				
				return project;
			case FILTER :
				System.out.println("==> Assembling Filter");
				SaberFilterRule filter = new SaberFilterRule(schema1, rel, queryId, timestampReference, window1, batchSize);
				filter.prepareRule();				
				return filter;			
			case JOIN :
				System.out.println("==> Assembling Join");
				SaberJoinRule join = new SaberJoinRule(schema1, schema2, rel, queryId, timestampReference, window1, window2, batchSize);
				join.prepareRule();				
				return join;							
			case AGGREGATE : 				
				System.out.println("==> Assembling Aggregate");
				SaberAggregateRule aggregate = new SaberAggregateRule(schema1, rel, queryId, timestampReference, window1, batchSize);
				aggregate.prepareRule();				
				return aggregate;			
			case SCAN :
				System.out.println("==> Assembling Scan");
				SaberScanRule scan = new SaberScanRule(schema1, queryId, timestampReference, flag, batchSize);
				scan.prepareRule();				
				return scan;
			case WINDOW :
				System.out.println("==> Assembling Window");
				SaberWindowRule window = new SaberWindowRule(schema1, rel, queryId, timestampReference, window1, batchSize);
				window.prepareRule();				
				return window;
			default :
				System.err.println("error: invalid rule");
				System.exit(1);
				return null;
		}		
	}
	

}
