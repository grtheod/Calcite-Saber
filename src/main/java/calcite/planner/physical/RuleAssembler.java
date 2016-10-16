package calcite.planner.physical;

import java.util.List;

import org.apache.calcite.util.Pair;

import calcite.planner.physical.rules.SaberAggregateRule;
import calcite.planner.physical.rules.SaberFilterRule;
import calcite.planner.physical.rules.SaberJoinRule;
import calcite.planner.physical.rules.SaberProjectRule;
import calcite.planner.physical.rules.SaberScanRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.SystemConf;

public class RuleAssembler {
	
	protected static final String PROJECT= "LogicalProject";
	protected static final String FILTER = "LogicalFilter";
	protected static final String JOIN = "LogicalJoin";	
	protected static final String AGGREGATE = "LogicalAggregate";
	protected static final String SCAN = "LogicalTableScan";
	String operator;
	String args;
	ITupleSchema schema1, schema2;
	int queryId;
	long timestampReference;
	
	public RuleAssembler(String operator, String args, ITupleSchema schema, int queryId, long timestampReference){
		this.operator = operator;
		this.args = args;
		this.schema1 = schema;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
	}
	
	public RuleAssembler(String operator, String args, ITupleSchema schema1, ITupleSchema schema2, int queryId, long timestampReference){
		this.operator = operator;
		this.args = args;
		this.schema1 = schema1;
		this.schema2 = schema2;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
	}
	
	public SaberRule construct(){
		
		Query query = null;
		ITupleSchema outputSchema = null;
		switch (operator){			
			case PROJECT :
				System.out.println("==> Assembling Projection");
				SaberProjectRule project = new SaberProjectRule(schema1, args, queryId, timestampReference);
				project.prepareRule();				
				return project;
			case FILTER :
				System.out.println("==> Assembling Filter");
				SaberFilterRule filter = new SaberFilterRule(schema1, args, queryId, timestampReference);
				filter.prepareRule();				
				return filter;
			case JOIN :
				System.out.println("==> Assembling Join");
				SaberJoinRule join = new SaberJoinRule(schema1, schema2, args, queryId, timestampReference);
				join.prepareRule();				
				return join;				
			case AGGREGATE :
				System.out.println("==> Assembling Aggregate");
				SaberAggregateRule aggregate = new SaberAggregateRule(schema1, args, queryId, timestampReference);
				aggregate.prepareRule();				
				return aggregate;	
			case SCAN :
				System.out.println("==> Assembling Scan");
				SaberScanRule scan = new SaberScanRule(schema1);
				scan.prepareRule();				
				return scan;
			default :
				
		}
		
		System.err.println("error: invalid rule");
		System.exit(1);
		return null;
	}
	

}
