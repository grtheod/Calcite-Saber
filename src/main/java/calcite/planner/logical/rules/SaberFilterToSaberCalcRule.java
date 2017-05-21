package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import calcite.planner.logical.SaberCalcRel;
import calcite.planner.logical.SaberFilterRel;

public class SaberFilterToSaberCalcRule extends RelOptRule{
	//~ Static fields/initializers ---------------------------------------------

	public static final SaberFilterToSaberCalcRule INSTANCE = new SaberFilterToSaberCalcRule();

	//~ Constructors -----------------------------------------------------------

	private SaberFilterToSaberCalcRule() {
		super(operand(SaberFilterRel.class, any()));
	}

	//~ Methods ----------------------------------------------------------------


	public void onMatch(RelOptRuleCall call) {
		final SaberFilterRel filter = call.rel(0);
		final RelNode rel = filter.getInput();

		// Create a program containing a filter.
		final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
		final RelDataType inputRowType = rel.getRowType();
		final RexProgramBuilder programBuilder =
				new RexProgramBuilder(inputRowType, rexBuilder);
		programBuilder.addIdentity();
		programBuilder.addCondition(filter.getCondition());
		final RexProgram program = programBuilder.getProgram();

		final SaberCalcRel calc = SaberCalcRel.create(rel, program);
		call.transformTo(calc);
	}
}
