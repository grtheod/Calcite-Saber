package calcite.planner.physical;

import java.util.List;

import org.apache.calcite.util.Pair;

import calcite.planner.physical.rules.SaberAggregateRule;
import calcite.planner.physical.rules.SaberFilterRule;
import calcite.planner.physical.rules.SaberProjectRule;
import calcite.planner.physical.rules.SaberScanRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;

public class RuleAssembler {
	
	protected static final String PROJECT= "LogicalProject";
	protected static final String FILTER = "LogicalFilter";
	protected static final String JOIN = "LogicalJoin";	
	protected static final String AGGREGATE = "LogicalAggregate";
	protected static final String SCAN = "LogicalTableScan";
	String operator;
	List <String> args;
	ITupleSchema schema;
	
	public RuleAssembler(String operator,List <String> args, ITupleSchema schema){
		this.operator=operator;
		this.args=args;
		this.schema=schema;
	}
	
	public SaberRule construct(){
		
		Query query = null;
		ITupleSchema outputSchema = null;
		switch (operator){			
			case PROJECT :
				System.out.println("==> Assembling Projection");
				SaberProjectRule project = new SaberProjectRule(schema,args);
				project.prepareRule();				
				return project;
			case FILTER :
				System.out.println("==> Assembling Filter");
				SaberFilterRule filter = new SaberFilterRule(schema,args);
				filter.prepareRule();				
				return filter;
			case JOIN :
				System.out.println("==> Assembling Join");
				System.err.println("Not implemented yet.");
				System.exit(1);
				break;				
			case AGGREGATE :
				System.out.println("==> Assembling Aggregate");
				SaberAggregateRule aggregate = new SaberAggregateRule(schema,args);
				aggregate.prepareRule();				
				return aggregate;	
			case SCAN :
				System.out.println("==> Assembling Scan");
				SaberScanRule scan = new SaberScanRule(schema,args);
				scan.prepareRule();				
				return scan;
			default :
				
		}
		
		System.err.println("error: invalid rule");
		System.exit(1);
		return null;
	}
	

}
