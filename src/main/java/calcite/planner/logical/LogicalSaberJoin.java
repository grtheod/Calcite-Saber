package calcite.planner.logical;

import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public final class LogicalSaberJoin  extends Join {

	  private final boolean semiJoinDone;
	  
	  private final ImmutableList<RelDataTypeField> systemFieldList;
	  
	  public LogicalSaberJoin(
		      RelOptCluster cluster,
		      RelTraitSet traitSet,
		      RelNode left,
		      RelNode right,
		      RexNode condition,
		      Set<CorrelationId> variablesSet,
		      JoinRelType joinType,
		      boolean semiJoinDone,
		      ImmutableList<RelDataTypeField> systemFieldList) {
		    super(cluster, traitSet, left, right, condition, variablesSet, joinType);
		    this.semiJoinDone = semiJoinDone;
		    this.systemFieldList = Preconditions.checkNotNull(systemFieldList);
	  }
	  
	  @Deprecated // to be removed before 2.0
	  public LogicalSaberJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
	      RelNode right, RexNode condition, JoinRelType joinType,
	      Set<String> variablesStopped, boolean semiJoinDone,
	      ImmutableList<RelDataTypeField> systemFieldList) {
	    this(cluster, traitSet, left, right, condition,
	        CorrelationId.setOf(variablesStopped), joinType, semiJoinDone,
	        systemFieldList);
	  }

	  @Deprecated // to be removed before 2.0
	  public LogicalSaberJoin(RelOptCluster cluster, RelNode left, RelNode right,
	      RexNode condition, JoinRelType joinType, Set<String> variablesStopped) {
	    this(cluster, cluster.traitSetOf(Convention.NONE), left, right, condition,
	        CorrelationId.setOf(variablesStopped), joinType, false,
	        ImmutableList.<RelDataTypeField>of());
	  }

	  @Deprecated // to be removed before 2.0
	  public LogicalSaberJoin(RelOptCluster cluster, RelNode left, RelNode right,
	      RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
	      boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList) {
	    this(cluster, cluster.traitSetOf(Convention.NONE), left, right, condition,
	        CorrelationId.setOf(variablesStopped), joinType, semiJoinDone,
	        systemFieldList);
	  }

	  public static LogicalSaberJoin create(RelNode left, RelNode right,
		    RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType,
		    boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList) {
		    final RelOptCluster cluster = left.getCluster();
		    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
		    return new LogicalSaberJoin(cluster, traitSet, left, right, condition,
		        variablesSet, joinType, semiJoinDone, systemFieldList);
	  }

	  @Deprecated // to be removed before 2.0
	  public static LogicalSaberJoin create(RelNode left, RelNode right,
			RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
		    boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList) {
		    return create(left, right, condition, CorrelationId.setOf(variablesStopped),
		        joinType, semiJoinDone, systemFieldList);
		  }

	  /** Creates a LogicalJoin. */
	  public static LogicalSaberJoin create(RelNode left, RelNode right,
		    RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
		    return create(left, right, condition, variablesSet, joinType, false,
		        ImmutableList.<RelDataTypeField>of());
	  }

	  @Deprecated // to be removed before 2.0
	  public static LogicalSaberJoin create(RelNode left, RelNode right,
		    RexNode condition, JoinRelType joinType, Set<String> variablesStopped) {
		    return create(left, right, condition, CorrelationId.setOf(variablesStopped),
		        joinType, false, ImmutableList.<RelDataTypeField>of());
	  }	  
	  
	  @Override public LogicalSaberJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
		    RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
		    assert traitSet.containsIfApplicable(Convention.NONE);
		    return new LogicalSaberJoin(getCluster(),
		        getCluster().traitSetOf(Convention.NONE), left, right, conditionExpr,
		        variablesSet, joinType, semiJoinDone, systemFieldList);
	  }

	  @Override public RelNode accept(RelShuttle shuttle) {
		    return shuttle.visit(this);
	  }

	  public RelWriter explainTerms(RelWriter pw) {
		    // Don't ever print semiJoinDone=false. This way, we
		    // don't clutter things up in optimizers that don't use semi-joins.
		    return super.explainTerms(pw)
		        .itemIf("semiJoinDone", semiJoinDone, semiJoinDone);
	  }

	  @Override public boolean isSemiJoinDone() {
		    return semiJoinDone;
	  }

	  public List<RelDataTypeField> getSystemFieldList() {
		    return systemFieldList;
	  }
	  
	  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
		      RelMetadataQuery mq) {
		    double rowCount = mq.getRowCount(this)*0.5;
		    return planner.getCostFactory().makeCost(rowCount, 0, 0);
	  } 
	
}
