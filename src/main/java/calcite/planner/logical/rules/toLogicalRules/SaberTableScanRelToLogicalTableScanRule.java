package calcite.planner.logical.rules.toLogicalRules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;

import calcite.planner.logical.SaberTableScanRel;

public class SaberTableScanRelToLogicalTableScanRule extends RelOptRule {
	public static final SaberTableScanRelToLogicalTableScanRule INSTANCE = new SaberTableScanRelToLogicalTableScanRule();

	  //~ Constructors -----------------------------------------------------------

	  private SaberTableScanRelToLogicalTableScanRule() {		  
		  super(operand(SaberTableScanRel.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override public void onMatch(RelOptRuleCall call) {
		    final SaberTableScanRel scan = call.rel(0);
		    final RelTraitSet traitSet = scan.getTraitSet();
		    final RelOptCluster cluster = scan.getCluster();
		    final RelOptTable table = scan.getTable();
		    
		    call.transformTo(
		        new LogicalTableScan( cluster,
		        		traitSet, table		        		
		        		));
		  }
	  
}
