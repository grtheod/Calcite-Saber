package calcite.planner.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

import calcite.planner.common.SaberFilterRelBase;

public class SaberFilterRel extends SaberFilterRelBase implements SaberRel {
	
	  public SaberFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
		    super(cluster, traits, child, condition);
	  }
	
	  @Override
	  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
		    return new SaberFilterRel(getCluster(), traitSet, input, condition);
	  }
	  
	  @Override
	  public <T> T accept(SaberRelVisitor<T> visitor) {
	    return null;
	  }

	  public static SaberFilterRel create(RelNode child, RexNode condition) {
		  return new SaberFilterRel(child.getCluster(), child.getTraitSet(), child, condition)  ;
	  }
}
