package calcite.planner.logical;

import org.apache.calcite.adapter.enumerable.EnumerableWindow;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalWindow;

public class EnumerableWindowToLogicalWindowRule extends RelOptRule {
	  public static final EnumerableWindowToLogicalWindowRule INSTANCE = new EnumerableWindowToLogicalWindowRule();

	  private EnumerableWindowToLogicalWindowRule() {
		  super(operand(EnumerableWindow.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override
	  public void onMatch(RelOptRuleCall call) {
	    final EnumerableWindow window = call.rel(0);
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
