package calcite.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import calcite.planner.common.SaberAggrCalcRelBase;
import calcite.planner.core.AggrCalc;

public class SaberAggrCalcRel extends SaberAggrCalcRelBase implements SaberRel {

	protected SaberAggrCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram previousProgram,
			RexProgram nextProgram, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
			List<AggregateCall> aggCalls) {
		super(cluster, traits, child, previousProgram, nextProgram, indicator, groupSet, groupSets, aggCalls);
	}

	@Override
	public <T> T accept(SaberRelVisitor<T> visitor) {
		return null;
	}

	@Override
	public AggrCalc copy(RelTraitSet traitSet, RelNode input, RexProgram previousProgram, RexProgram nextProgram,
			boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
			List<AggregateCall> aggCalls) {
		return new SaberAggrCalcRel(getCluster(), traitSet, input, previousProgram, nextProgram, indicator, groupSet, groupSets, aggCalls);
	}
	
	public static SaberAggrCalcRel create(RelNode child, RexProgram previousProgram, RexProgram nextProgram,
			boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
			List<AggregateCall> aggCalls) {
	    return new SaberAggrCalcRel(child.getCluster(), child.getTraitSet(), child, previousProgram, nextProgram, indicator,groupSet, groupSets, aggCalls);
	}

}
