package calcite.planner.logical.rules;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class EnumerableTableScanToLogicalTableScanRule extends RelOptRule {
	public static final EnumerableTableScanToLogicalTableScanRule INSTANCE = new EnumerableTableScanToLogicalTableScanRule();

	  //~ Constructors -----------------------------------------------------------

	  private EnumerableTableScanToLogicalTableScanRule() {		  
		  super(operand(EnumerableTableScan.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override public void onMatch(RelOptRuleCall call) {
		    final EnumerableTableScan scan = call.rel(0);
		    final RelTraitSet traitSet = scan.getTraitSet();
		    final RelOptCluster cluster = scan.getCluster();
		    final RelOptTable table = scan.getTable();
		    
		    call.transformTo(
		        new LogicalTableScan( cluster,
		        		traitSet, table		        		
		        		));
		  }
	  
}
