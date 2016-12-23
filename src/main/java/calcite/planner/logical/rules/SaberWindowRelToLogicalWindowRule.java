package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalWindow;

import calcite.planner.logical.SaberWindowRel;

public class SaberWindowRelToLogicalWindowRule extends RelOptRule {
	  public static final SaberWindowRelToLogicalWindowRule INSTANCE = new SaberWindowRelToLogicalWindowRule();

	  private SaberWindowRelToLogicalWindowRule() {
		  super(operand(SaberWindowRel.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override
	  public void onMatch(RelOptRuleCall call) {
	    final SaberWindowRel window = call.rel(0);
	    //final RelNode input = call.rel(1);
	    final RelTraitSet traits = window.getTraitSet();
	    call.transformTo(
	        new LogicalWindow(
	            window.getCluster(),
	            traits,
	            window.getInput(),
	            window.constants,
	            window.getRowType(),
	            window.groups));
	  }
	}
