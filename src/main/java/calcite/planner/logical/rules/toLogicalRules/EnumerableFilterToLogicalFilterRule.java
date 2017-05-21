package calcite.planner.logical.rules.toLogicalRules;

import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

import com.google.common.collect.ImmutableSet;

public class EnumerableFilterToLogicalFilterRule extends RelOptRule {
	  public static final EnumerableFilterToLogicalFilterRule INSTANCE = new EnumerableFilterToLogicalFilterRule();

	  private EnumerableFilterToLogicalFilterRule() {
		  super(operand(EnumerableFilter.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override
	  public void onMatch(RelOptRuleCall call) {
	    final EnumerableFilter filter = (EnumerableFilter) call.rel(0);
	    final RelNode input = filter.getInput();	    
	    call.transformTo(new LogicalFilter(filter.getCluster(), 
	    		input.getTraitSet(), input, filter.getCondition(), 
	    		ImmutableSet.<CorrelationId>of()));
	  }
	}
