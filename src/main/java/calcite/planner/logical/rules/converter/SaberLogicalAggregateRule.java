package calcite.planner.logical.rules.converter;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

import calcite.planner.logical.SaberAggregateRel;
import calcite.planner.logical.SaberRel;

public class SaberLogicalAggregateRule extends ConverterRule {
	
	  public static final RelOptRule INSTANCE = new SaberLogicalAggregateRule();

	  private SaberLogicalAggregateRule() {
	    super(LogicalAggregate.class, Convention.NONE, SaberRel.SABER_LOGICAL, "SaberAggregateRule");
	  }

	  @Override
	  public RelNode convert(RelNode rel) {
	    final LogicalAggregate aggregate = (LogicalAggregate)rel;
	    final RelNode input = aggregate.getInput();

	    return new SaberAggregateRel(aggregate.getCluster(),
	        aggregate.getTraitSet().replace(SaberRel.SABER_LOGICAL),
	        convert(input, input.getTraitSet().replace(SaberRel.SABER_LOGICAL)),
	        aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
	  }
	
	
}
