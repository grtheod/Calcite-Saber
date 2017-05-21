package calcite.planner.logical.rules.toLogicalRules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;

import calcite.planner.logical.SaberAggregateRel;

public class SaberAggregateRelToLogicalAggregateRule extends RelOptRule {
	  public static final SaberAggregateRelToLogicalAggregateRule INSTANCE = new SaberAggregateRelToLogicalAggregateRule();

	  private SaberAggregateRelToLogicalAggregateRule() {
		  super(operand(SaberAggregateRel.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override
	  public void onMatch(RelOptRuleCall call) {
	    final SaberAggregateRel aggregate = (SaberAggregateRel) call.rel(0);
	    call.transformTo(new LogicalAggregate(aggregate.getCluster(), 
	    		aggregate.getTraitSet(), aggregate.getInput(), aggregate.indicator,
	            aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList()
	    		));
	  }
	}

