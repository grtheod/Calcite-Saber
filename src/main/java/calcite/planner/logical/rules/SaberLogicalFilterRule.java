package calcite.planner.logical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

import calcite.planner.logical.SaberFilterRel;
import calcite.planner.logical.SaberRel;

public class SaberLogicalFilterRule extends ConverterRule {
	  public static SaberLogicalFilterRule INSTANCE = new SaberLogicalFilterRule();

	  private SaberLogicalFilterRule() {
	    super(LogicalFilter.class, Convention.NONE, SaberRel.SABER_LOGICAL, "SaberFilterRule");
	  }

	  @Override
	  public RelNode convert(RelNode rel) {
	    final Filter filter = (Filter) rel;
	    final RelNode input = filter.getInput();

	    return new SaberFilterRel(filter.getCluster(),
	        filter.getTraitSet().replace(SaberRel.SABER_LOGICAL),
	        convert(input, input.getTraitSet().replace(SaberRel.SABER_LOGICAL)),
	        filter.getCondition());
	  }
			  	
}
