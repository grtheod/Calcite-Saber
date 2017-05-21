package calcite.planner.logical.rules.toLogicalRules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;

import calcite.planner.logical.SaberCalcRel;

public class SaberCalcToLogicalCalcRule extends RelOptRule {
	  public static final SaberCalcToLogicalCalcRule INSTANCE = new SaberCalcToLogicalCalcRule();

	  private SaberCalcToLogicalCalcRule() {
		  super(operand(SaberCalcRel.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	@SuppressWarnings("deprecation")
	@Override
	  public void onMatch(RelOptRuleCall call) {
	    final SaberCalcRel calc = (SaberCalcRel) call.rel(0);	    
	    call.transformTo(new LogicalCalc(calc.getCluster(), 
	    		calc.getTraitSet(), calc.getInput(), calc.getProgram(), 
	    		calc.getCollationList()));
	  }
}
