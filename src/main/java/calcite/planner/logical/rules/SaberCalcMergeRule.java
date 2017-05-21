package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import calcite.planner.logical.SaberCalcRel;

public class SaberCalcMergeRule extends RelOptRule {
	//~ Static fields/initializers ---------------------------------------------

	public static final SaberCalcMergeRule INSTANCE = new SaberCalcMergeRule();

	//~ Constructors -----------------------------------------------------------
	
	private SaberCalcMergeRule() {
		super(
	        operand(
	            SaberCalcRel.class,
	            operand(SaberCalcRel.class, any())));
	}

	//~ Methods ----------------------------------------------------------------

	public void onMatch(RelOptRuleCall call) {
	    final Calc topCalc = call.rel(0);
	    final Calc bottomCalc = call.rel(1);

	    // Don't merge a calc which contains windowed aggregates onto a
	    // calc. That would effectively be pushing a windowed aggregate down
	    // through a filter.
	    RexProgram topProgram = topCalc.getProgram();
	    if (RexOver.containsOver(topProgram)) {
	      return;
	    }

	    // Merge the programs together.

	    RexProgram mergedProgram =
	        RexProgramBuilder.mergePrograms(
	            topCalc.getProgram(),
	            bottomCalc.getProgram(),
	            topCalc.getCluster().getRexBuilder());
	    assert mergedProgram.getOutputRowType()
	        == topProgram.getOutputRowType();
	    final Calc newCalc =
	        topCalc.copy(
	            topCalc.getTraitSet(),
	            bottomCalc.getInput(),
	            mergedProgram);

	    if (newCalc.getDigest().equals(bottomCalc.getDigest())) {
	      // newCalc is equivalent to bottomCalc, which means that topCalc
	      // must be trivial. Take it out of the game.
	      call.getPlanner().setImportance(topCalc, 0.0);
	    }

	    call.transformTo(newCalc);
	}
}
