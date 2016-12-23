package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;

import calcite.planner.logical.SaberProjectRel;

public class SaberProjectRelToLogicalProjectRule extends RelOptRule {
	public static final SaberProjectRelToLogicalProjectRule INSTANCE = new SaberProjectRelToLogicalProjectRule();

	  //~ Constructors -----------------------------------------------------------

	  private SaberProjectRelToLogicalProjectRule() {		  
		  super(operand(SaberProjectRel.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override public void onMatch(RelOptRuleCall call) {
		    final SaberProjectRel project = call.rel(0);
		    final RelNode input = project.getInput();
		    final RelTraitSet traits = project.getTraitSet();
		    
		    call.transformTo(
		        new LogicalProject( project.getCluster(),
		        	traits, input, project.getProjects(),
		        	project.getRowType()
		        		));
		  }
	  
}
