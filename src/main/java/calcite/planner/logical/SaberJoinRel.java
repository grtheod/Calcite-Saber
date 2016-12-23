package calcite.planner.logical;

import java.util.Collections;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import calcite.planner.common.SaberJoinRelBase;

public class SaberJoinRel extends SaberJoinRelBase implements SaberRel {


	  public SaberJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
		      JoinRelType joinType){
		super(cluster, traits, left, right, condition, joinType);
	  }

	  @Override
	  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right,
	                   JoinRelType joinType, boolean semiJoinDone) {
	    final JoinInfo joinInfo = JoinInfo.of(left, right, condition);

	    return new SaberJoinRel(getCluster(), traitSet, left, right, conditionExpr, joinType);
	  }
	  
	  @Override
	  public <T> T accept(SaberRelVisitor<T> visitor) {
	    return null;
	  }
}
