package calcite.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import calcite.planner.common.SaberProjectRelBase;

public class SaberProjectRel extends SaberProjectRelBase implements SaberRel {
	
	public SaberProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
	              List<? extends RexNode> projects, RelDataType rowType) {
		super(cluster, traits, input, projects, rowType);
	}
	
	public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
		return new SaberProjectRel(getCluster(), traitSet, input, projects, rowType);
	}
		
	@Override
	public <T> T accept(SaberRelVisitor<T> visitor) {
		return null;
	}

	public static SaberProjectRel create(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<? extends RexNode> exps,
              RelDataType rowType) {
		return new SaberProjectRel(cluster, traits, child, exps, rowType);
	}	
}
