package calcite.planner.logical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;

import calcite.planner.logical.SaberRel;
import calcite.planner.logical.SaberWindowRel;

public class SaberLogicalWindowRule extends ConverterRule {
	
	  public static final SaberLogicalWindowRule INSTANCE = new SaberLogicalWindowRule();

	  private SaberLogicalWindowRule() {
	    super(LogicalWindow.class, Convention.NONE, SaberRel.SABER_LOGICAL, "SaberWindowRule");
	  }

	  @Override
	  public RelNode convert(RelNode rel) {
	    final Window window = (Window) rel;
	    final RelNode input = window.getInput();

	    return new SaberWindowRel(window.getCluster(),
	        window.getTraitSet().replace(SaberRel.SABER_LOGICAL),
	        convert(input, input.getTraitSet().replace(SaberRel.SABER_LOGICAL)),
	        window.constants,
	        window.getRowType(),
	        window.groups);
	  }
}
