package calcite.planner.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class SaberProjectJoinTransposeRule2 extends RelOptRule {
	  public static final SaberProjectJoinTransposeRule2 INSTANCE =
	      new SaberProjectJoinTransposeRule2(
	          PushProjector.ExprCondition.FALSE,
	          RelFactories.LOGICAL_BUILDER);

	  //~ Instance fields --------------------------------------------------------

	  /**
	   * Condition for expressions that should be preserved in the projection.
	   */
	  private final PushProjector.ExprCondition preserveExprCondition;

	  //~ Constructors -----------------------------------------------------------

	  /**
	   * Creates a SaberProjectJoinTransposeRule with an explicit condition.
	   *
	   * @param preserveExprCondition Condition for expressions that should be
	   *                              preserved in the projection
	   */
	  public SaberProjectJoinTransposeRule2(
	      PushProjector.ExprCondition preserveExprCondition,
	      RelBuilderFactory relFactory) {
	    super(
	        operand(Project.class,
	            operand(EnumerableJoin.class, any())),
	        relFactory, null);
	    this.preserveExprCondition = preserveExprCondition;
	  }

	  //~ Methods ----------------------------------------------------------------

	  // implement RelOptRule
	  public void onMatch(RelOptRuleCall call) {
	    Project origProj = call.rel(0);
	    final EnumerableJoin join = call.rel(1);

	    // locate all fields referenced in the projection and join condition;
	    // determine which inputs are referenced in the projection and
	    // join condition; if all fields are being referenced and there are no
	    // special expressions, no point in proceeding any further
	    PushProjector pushProject =
	        new PushProjector(
	            origProj,
	            join.getCondition(),
	            join,
	            preserveExprCondition,
	            relBuilderFactory.create(origProj.getCluster(), null));
	    if (pushProject.locateAllRefs()) {
	      return;
	    }

	    // create left and right projections, projecting only those
	    // fields referenced on each side
	    RelNode leftProjRel =
	        pushProject.createProjectRefsAndExprs(
	            join.getLeft(),
	            true,
	            false);
	    RelNode rightProjRel =
	        pushProject.createProjectRefsAndExprs(
	            join.getRight(),
	            true,
	            true);

	    // convert the join condition to reference the projected columns
	    RexNode newJoinFilter = null;
	    int[] adjustments = pushProject.getAdjustments();
	    if (join.getCondition() != null) {
	      List<RelDataTypeField> projJoinFieldList =
	          new ArrayList<RelDataTypeField>();
	      projJoinFieldList.addAll(
	          join.getSystemFieldList());
	      projJoinFieldList.addAll(
	          leftProjRel.getRowType().getFieldList());
	      projJoinFieldList.addAll(
	          rightProjRel.getRowType().getFieldList());
	      newJoinFilter =
	          pushProject.convertRefsAndExprs(
	              join.getCondition(),
	              projJoinFieldList,
	              adjustments);
	    }

	    // create a new join with the projected children
	    EnumerableJoin newJoinRel =
	        join.copy(
	            join.getTraitSet(),
	            newJoinFilter,
	            leftProjRel,
	            rightProjRel,
	            join.getJoinType(),
	            join.isSemiJoinDone());

	    // put the original project on top of the join, converting it to
	    // reference the modified projection list
	    RelNode topProject =
	        pushProject.createNewProject(newJoinRel, adjustments);

	    call.transformTo(topProject);
	  }
	}