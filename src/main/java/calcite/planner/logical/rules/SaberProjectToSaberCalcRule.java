package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexProgram;

import calcite.planner.logical.SaberCalcRel;
import calcite.planner.logical.SaberProjectRel;

public class SaberProjectToSaberCalcRule extends RelOptRule {
	//~ Static fields/initializers ---------------------------------------------
	
	public static final SaberProjectToSaberCalcRule INSTANCE = new SaberProjectToSaberCalcRule();

	//~ Constructors -----------------------------------------------------------

	private SaberProjectToSaberCalcRule() {
		super(operand(SaberProjectRel.class, any()));
	}
	
	//~ Methods ----------------------------------------------------------------

	public void onMatch(RelOptRuleCall call) {
		final SaberProjectRel project = call.rel(0);
		final RelNode input = project.getInput();
		final RexProgram program =
				RexProgram.create(
						input.getRowType(),
						project.getProjects(),
						null,
						project.getRowType(),
						project.getCluster().getRexBuilder());
		final SaberCalcRel calc = SaberCalcRel.create(input, program);
		call.transformTo(calc);
  }

}
