package calcite.planner.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class ProjectJoinRemoveRule extends RelOptRule {

	public static final ProjectJoinRemoveRule INSTANCE =
			new ProjectJoinRemoveRule(PushProjector.ExprCondition.FALSE,
			          RelFactories.LOGICAL_BUILDER);
	
	private final PushProjector.ExprCondition preserveExprCondition;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a ProjectJoinTransposeRule with an explicit condition.
	 *
	 * @param preserveExprCondition Condition for expressions that should be
	 *                              preserved in the projection
	 */
	public ProjectJoinRemoveRule(
	    PushProjector.ExprCondition preserveExprCondition,
	    RelBuilderFactory relFactory) {
	  super(
	      operand(Project.class,
	          operand(Join.class, any())),
	      relFactory, null);
	  this.preserveExprCondition = preserveExprCondition;
	}
    //~ Methods ----------------------------------------------------------------

	// implement RelOptRule	
	public void onMatch(RelOptRuleCall call) {
	    Project origProj = call.rel(0);
	    final Join join = call.rel(1);
	    if (join instanceof SemiJoin) {
	        return; // TODO: support SemiJoin
	    }
	    if (join instanceof SemiJoin) {
	    	return; // TODO: support SemiJoin
	    }
	    // locate all fields referenced in the projection and join condition;
	    // determine which inputs are referenced in the projection and
	    // join condition; if all fields are being referenced and there are no
	    // special expressions, no point in proceeding any further
	    System.out.println("aaa "+origProj.getExpectedInputRowType(1));
	    System.out.println("aaa "+join.getExpectedInputRowType(0));

	    if (origProj.getExpectedInputRowType(0) == join.getExpectedInputRowType(0)) {
	    	System.out.println("aaa"+join.getChildExps());
	    }
	    return;
	}
		
}
