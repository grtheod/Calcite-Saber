package calcite.planner.logical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;

import calcite.planner.logical.SaberLogicalConvention;
import calcite.planner.logical.SaberProjectRel;
import calcite.planner.logical.SaberRel;

public class SaberLogicalProjectRule extends ConverterRule {
	
	  public static final SaberLogicalProjectRule INSTANCE = new SaberLogicalProjectRule();

	  private SaberLogicalProjectRule() {
	    super(LogicalProject.class, Convention.NONE, SaberRel.SABER_LOGICAL,
	        "SaberProjectRule");
	  }

	  @Override
	  public RelNode convert(RelNode rel) {
	    final Project project = (Project) rel;
	    final RelNode input = project.getInput();
	    return new SaberProjectRel(project.getCluster(),
	        project.getTraitSet().replace(SaberRel.SABER_LOGICAL),
	        convert(input, input.getTraitSet().replace(SaberRel.SABER_LOGICAL)), project.getProjects(), project.getRowType());
	  }
}
