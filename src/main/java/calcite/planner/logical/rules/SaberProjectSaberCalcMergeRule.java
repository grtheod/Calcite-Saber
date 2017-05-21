package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Pair;

import calcite.planner.logical.SaberCalcRel;
import calcite.planner.logical.SaberProjectRel;

public class SaberProjectSaberCalcMergeRule extends RelOptRule {
	//~ Static fields/initializers ---------------------------------------------

	public static final SaberProjectSaberCalcMergeRule INSTANCE =
		new SaberProjectSaberCalcMergeRule();

	//~ Constructors -----------------------------------------------------------

	private SaberProjectSaberCalcMergeRule() {
	    super(
	    	operand(
	    		SaberProjectRel.class,
	            operand(SaberCalcRel.class, any())));
	}

	//~ Methods ----------------------------------------------------------------

	public void onMatch(RelOptRuleCall call) {
		final SaberProjectRel project = call.rel(0);
	    final SaberCalcRel calc = call.rel(1);

	    // Don't merge a project which contains windowed aggregates onto a
	    // calc. That would effectively be pushing a windowed aggregate down
	    // through a filter. Transform the project into an identical calc,
	    // which we'll have chance to merge later, after the over is
	    // expanded.
	    final RelOptCluster cluster = project.getCluster();
	    RexProgram program =
	        RexProgram.create(
	            calc.getRowType(),
	            project.getProjects(),
	            null,
	            project.getRowType(),
	            cluster.getRexBuilder());
	    if (RexOver.containsOver(program)) {
	    	SaberCalcRel projectAsCalc = SaberCalcRel.create(calc, program);
	      call.transformTo(projectAsCalc);
	      return;
	    }

	    // Create a program containing the project node's expressions.
	    final RexBuilder rexBuilder = cluster.getRexBuilder();
	    final RexProgramBuilder progBuilder =
	        new RexProgramBuilder(
	            calc.getRowType(),
	            rexBuilder);
	    for (Pair<RexNode, String> field : project.getNamedProjects()) {
	      progBuilder.addProject(field.left, field.right);
	    }
	    RexProgram topProgram = progBuilder.getProgram();
	    RexProgram bottomProgram = calc.getProgram();

	    // Merge the programs together.
	    RexProgram mergedProgram =
	        RexProgramBuilder.mergePrograms(
	            topProgram,
	            bottomProgram,
	            rexBuilder);
	    final SaberCalcRel newCalc =
	    		SaberCalcRel.create(calc.getInput(), mergedProgram);
	    call.transformTo(newCalc);
	}

}
