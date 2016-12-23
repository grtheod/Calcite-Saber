package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

import com.google.common.collect.ImmutableSet;

import calcite.planner.logical.SaberFilterRel;

public class SaberFilterRelToLogicalFilterRule extends RelOptRule {
	  public static final SaberFilterRelToLogicalFilterRule INSTANCE = new SaberFilterRelToLogicalFilterRule();

	  private SaberFilterRelToLogicalFilterRule() {
		  super(operand(SaberFilterRel.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override
	  public void onMatch(RelOptRuleCall call) {
	    final SaberFilterRel filter = (SaberFilterRel) call.rel(0);
	    final RelNode input = filter.getInput();	    
	    call.transformTo(new LogicalFilter(filter.getCluster(), 
	    		input.getTraitSet(), input, filter.getCondition(), 
	    		ImmutableSet.<CorrelationId>of()));
	  }
	}
