package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import calcite.planner.logical.SaberCalcRel;
import calcite.planner.logical.SaberFilterRel;

public class SaberFilterSaberCalcMergeRule extends RelOptRule {
	//~ Static fields/initializers ---------------------------------------------

	public static final SaberFilterSaberCalcMergeRule INSTANCE =
		new SaberFilterSaberCalcMergeRule();

	//~ Constructors -----------------------------------------------------------

	private SaberFilterSaberCalcMergeRule() {
		super(
			operand(
	        Filter.class,
	        operand(SaberCalcRel.class, any())));
	}

	//~ Methods ----------------------------------------------------------------

	public void onMatch(RelOptRuleCall call) {
		final SaberFilterRel filter = call.rel(0);
		final SaberCalcRel calc = call.rel(1);

		// Don't merge a filter onto a calc which contains windowed aggregates.
		// That would effectively be pushing a multiset down through a filter.
		// We'll have chance to merge later, when the over is expanded.
		if (calc.getProgram().containsAggs()) {
			return;
		}

		// Create a program containing the filter.
		final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
		final RexProgramBuilder progBuilder =
			new RexProgramBuilder(
	          calc.getRowType(),
	          rexBuilder);
		progBuilder.addIdentity();
		progBuilder.addCondition(filter.getCondition());
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
