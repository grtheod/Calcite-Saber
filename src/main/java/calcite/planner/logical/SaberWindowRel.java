package calcite.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import calcite.planner.common.SaberWindowRelBase;

public class SaberWindowRel extends SaberWindowRelBase implements SaberRel {
	
	public SaberWindowRel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
              List<RexLiteral> constants, RelDataType rowType, List<Window.Group> groups) {
		super(cluster, traits, child, constants, rowType, groups);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new SaberWindowRel(getCluster(), traitSet, sole(inputs), constants, getRowType(), groups);
	}

    @Override
    public <T> T accept(SaberRelVisitor<T> visitor) {
    	return null;
    }	
}
