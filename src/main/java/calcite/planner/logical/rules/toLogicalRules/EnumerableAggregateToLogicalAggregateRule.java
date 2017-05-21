package calcite.planner.logical.rules.toLogicalRules;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class EnumerableAggregateToLogicalAggregateRule extends RelOptRule {
	  public static final EnumerableAggregateToLogicalAggregateRule INSTANCE = new EnumerableAggregateToLogicalAggregateRule();

	  private EnumerableAggregateToLogicalAggregateRule() {
		  super(operand(EnumerableAggregate.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override
	  public void onMatch(RelOptRuleCall call) {
	    final EnumerableAggregate aggregate = (EnumerableAggregate) call.rel(0);
	    call.transformTo(new LogicalAggregate(aggregate.getCluster(), 
	    		aggregate.getTraitSet(), aggregate.getInput(), aggregate.indicator,
	            aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList()
	    		));
	  }
	}

