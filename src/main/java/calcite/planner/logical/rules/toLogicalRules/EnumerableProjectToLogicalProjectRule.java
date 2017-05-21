package calcite.planner.logical.rules.toLogicalRules;

import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;

public class EnumerableProjectToLogicalProjectRule extends RelOptRule {
	public static final EnumerableProjectToLogicalProjectRule INSTANCE = new EnumerableProjectToLogicalProjectRule();

	  //~ Constructors -----------------------------------------------------------

	  private EnumerableProjectToLogicalProjectRule() {		  
		  super(operand(EnumerableProject.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  @Override public void onMatch(RelOptRuleCall call) {
		    final EnumerableProject project = call.rel(0);
		    final RelNode input = project.getInput();
		    final RelTraitSet traits = project.getTraitSet();
		    
		    call.transformTo(
		        new LogicalProject( project.getCluster(),
		        	traits, input, project.getProjects(),
		        	project.getRowType()
		        		));
		  }
	  
}
