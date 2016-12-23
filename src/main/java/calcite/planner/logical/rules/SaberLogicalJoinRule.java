package calcite.planner.logical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;

import calcite.planner.logical.SaberJoinRel;
import calcite.planner.logical.SaberRel;

public class SaberLogicalJoinRule extends ConverterRule {
	  public static final SaberLogicalJoinRule INSTANCE = new SaberLogicalJoinRule();

	  private SaberLogicalJoinRule() {
	    super(LogicalJoin.class, Convention.NONE, SaberRel.SABER_LOGICAL, "SaberJoinRule");
	  }

	  @Override
	  public RelNode convert(RelNode rel) {
	    final Join join = (Join) rel;
	    final RelOptCluster cluster = join.getCluster();
	    final RelTraitSet traitSet =
	        join.getTraitSet().replace(SaberRel.SABER_LOGICAL);
	    final RelNode leftInput = join.getLeft();
	    final RelNode rightInput = join.getRight();
	    final RelNode left = convert(leftInput, leftInput.getTraitSet().replace(SaberRel.SABER_LOGICAL));
	    final RelNode right = convert(rightInput, rightInput.getTraitSet().replace(SaberRel.SABER_LOGICAL));

	    RelNode newRel =  new SaberJoinRel(join.getCluster(),
	        join.getTraitSet().replace(SaberRel.SABER_LOGICAL),
	        left,
	        right,
	        join.getCondition(),
	        join.getJoinType());


	    return newRel;
	  }
}
