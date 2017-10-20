package calcite.planner.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexProgram;

import calcite.planner.common.SaberCalcRelBase;

public class SaberCalcRel extends SaberCalcRelBase implements SaberRel {
	
	  public SaberCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram program) {
		    super(cluster, traits, child, program);
	  }
	
	  @Override
	  public Calc copy(RelTraitSet traitSet, RelNode input, RexProgram program) {
		    return new SaberCalcRel(getCluster(), traitSet, input, program);
	  }
	  
	  @Override
	  public <T> T accept(SaberRelVisitor<T> visitor) {
	    return null;
	  }

	  public static SaberCalcRel create(RelNode child, RexProgram program) {
		  return new SaberCalcRel(child.getCluster(), child.getTraitSet(), child, program)  ;
	  }

}
