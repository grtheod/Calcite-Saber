package calcite.planner.logical;

import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

public class SaberRelFactories {

	  public static final RelFactories.ProjectFactory SABER_LOGICAL_PROJECT_FACTORY =
	      new SaberProjectFactoryImpl();

	  public static final RelFactories.FilterFactory SABER_LOGICAL_FILTER_FACTORY =
	      new SaberFilterFactoryImpl();

	  public static final RelFactories.JoinFactory SABER_LOGICAL_JOIN_FACTORY = new SaberJoinFactoryImpl();

	  public static final SaberAggregateFactoryImpl SABER_LOGICAL_AGGREGATE_FACTORY = new SaberAggregateFactoryImpl();

	  public static final RelBuilderFactory SABER_LOGICAL_BUILDER =
		      RelBuilder.proto(
		          Contexts.of(SABER_LOGICAL_FILTER_FACTORY,
		        		  SABER_LOGICAL_JOIN_FACTORY,
		        		  SABER_LOGICAL_AGGREGATE_FACTORY,
		        		  SABER_LOGICAL_PROJECT_FACTORY
		        		  ));
	  /**
	   * Implementation of {@link RelFactories.ProjectFactory} that returns a vanilla
	   * {@link org.apache.calcite.rel.logical.LogicalProject}.
	   */
	  private static class SaberProjectFactoryImpl implements RelFactories.ProjectFactory {
	    @Override
	    public RelNode createProject(RelNode child,
	                                 List<? extends RexNode> childExprs, List<String> fieldNames) {
	      final RelOptCluster cluster = child.getCluster();
	      final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames);
	      final RelNode project = SaberProjectRel.create(cluster, child.getTraitSet(), child, childExprs, rowType);

	      return project;
	    }
	  }


	  /**
	   * Implementation of {@link RelFactories.FilterFactory} that
	   * returns a vanilla {@link LogicalFilter}.
	   */
	  private static class SaberFilterFactoryImpl implements RelFactories.FilterFactory {
	    @Override
	    public RelNode createFilter(RelNode child, RexNode condition) {
	      return SaberFilterRel.create(child, condition);
	    }
	  }


	  /**
	   * Implementation of {@link RelFactories.JoinFactory} that returns a vanilla
	   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
	   */
	  private static class SaberJoinFactoryImpl implements RelFactories.JoinFactory {
	    @Override
	    public RelNode createJoin(RelNode left, RelNode right,
	                              RexNode condition, JoinRelType joinType,
	                              Set<String> variablesStopped, boolean semiJoinDone) {
	      return new SaberJoinRel(left.getCluster(), left.getTraitSet(), left, right, condition, joinType);
	    }


		public RelNode createJoin(RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
				JoinRelType joinType, boolean semiJoinDone) {
			return new SaberJoinRel(left.getCluster(), left.getTraitSet(), left, right, condition, joinType);
		}
	  }


	  private static class SaberAggregateFactoryImpl implements RelFactories.AggregateFactory {
		    public RelNode createAggregate(RelNode input, boolean indicator,
		            ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
		            List<AggregateCall> aggCalls) {
		          return SaberAggregateRel.create(input, indicator,
		              groupSet, groupSets, aggCalls);
		    }
	  }
}
