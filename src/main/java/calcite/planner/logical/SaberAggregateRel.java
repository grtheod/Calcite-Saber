package calcite.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import calcite.planner.common.SaberAggregateRelBase;

public class SaberAggregateRel extends SaberAggregateRelBase implements SaberRel {

	  public SaberAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
	                           boolean indicator, ImmutableBitSet groupSet,
	                           List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
	    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
	  }

	  @Override
	  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator,
	                        ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
	                        List<AggregateCall> aggCalls) {
	    return new SaberAggregateRel(getCluster(), traitSet, input, indicator, groupSet, groupSets,
	        aggCalls);
	  }
	
	  @Override
	  public <T> T accept(SaberRelVisitor<T> visitor) {
	    return null;
	  }
	  
	  public static SaberAggregateRel create(RelNode input, boolean indicator,
              ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
              List<AggregateCall> aggCalls) {
		  return new SaberAggregateRel(input.getCluster(), input.getTraitSet(), input, indicator, groupSet, groupSets,
				  aggCalls);
	  }	  
	
}
