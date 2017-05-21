package calcite.planner.logical.rules.toLogicalRules;

import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

public class EnumerableJoinToLogicalJoinRule  extends RelOptRule {

	  public static final EnumerableJoinToLogicalJoinRule INSTANCE = new EnumerableJoinToLogicalJoinRule();

	  //~ Constructors -----------------------------------------------------------

	  private EnumerableJoinToLogicalJoinRule() {		  
		  super(operand(EnumerableJoin.class, any()), RelFactories.LOGICAL_BUILDER, null);
	  }

	  public boolean matches(RelOptRuleCall call) {
		    EnumerableJoin join = call.rel(0);
		    switch (join.getJoinType()) {
		    case INNER:
		      return true;
		    default:
		      throw Util.unexpected(join.getJoinType());
		    }
	  }	 
	  @Override
	  public void onMatch(RelOptRuleCall call) {
		    assert matches(call);
		    final EnumerableJoin join = call.rel(0);
		    RelNode right = join.getRight();
		    final RelNode left = join.getLeft();
		    final RexNode condition = join.getCondition();
		    final Set<CorrelationId> variablesSet = join.getVariablesSet();
		    final JoinRelType joinType = join.getJoinType();
		    final ImmutableList<RelDataTypeField> systemFieldList = ImmutableList.of();//(ImmutableList<RelDataTypeField>) join.getSystemFieldList();

		    RelNode newRel =
		        LogicalJoin.create(left,
		            right, condition, variablesSet,  
		            joinType, false, systemFieldList
		        	);
		    call.transformTo(newRel);
	  }
		
	  	
}
