package calcite.planner.core;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public abstract class AggrCalc extends SingleRel{
	//~ Instance fields --------------------------------------------------------

	protected final RexProgram previousProgram, nextProgram;
	public final boolean indicator;
	protected final List<AggregateCall> aggCalls;
	protected final ImmutableBitSet groupSet;
	public final ImmutableList<ImmutableBitSet> groupSets;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a AggCalc.
	 *
	 * @param cluster Cluster
	 * @param traits Traits
	 * @param child Input relation
	 * @param previousProgram Calc program before the aggregate
	 * @param nextProgram Calc program after the aggregate
	 * @param indicator Whether row type should include indicator fields to
     *                 indicate which grouping set is active; must be true if
     *                 aggregate is not simple
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just {@code groupSet}
     * @param aggCalls Collection of calls to aggregate functions
	 */
	protected AggrCalc(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram previousProgram, RexProgram nextProgram, 
		      boolean indicator,
		      ImmutableBitSet groupSet,
		      List<ImmutableBitSet> groupSets,
		      List<AggregateCall> aggCalls) {
		super(cluster, traits, child);
	    this.rowType = nextProgram.getOutputRowType(); // to pass the transformation check
	    this.previousProgram = previousProgram;
	    this.nextProgram = nextProgram;
	    assert isValid(Litmus.THROW, null);
	    this.indicator = indicator;
	    this.aggCalls = ImmutableList.copyOf(aggCalls);
	    this.groupSet = Preconditions.checkNotNull(groupSet);
	    if (groupSets == null) {
	      this.groupSets = ImmutableList.of(groupSet);
	    } else {
	      this.groupSets = ImmutableList.copyOf(groupSets);
	      assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
	      for (ImmutableBitSet set : groupSets) {
	        assert groupSet.contains(set);
	      }
	    }
	}
	//~ Methods ----------------------------------------------------------------

	@Override public final RelNode copy(RelTraitSet traitSet,
		List<RelNode> inputs) {
		return copy(traitSet, sole(inputs), previousProgram, nextProgram, indicator, groupSet, groupSets,
	        aggCalls);
	}

	public abstract AggrCalc copy(RelTraitSet traitSet, RelNode input, RexProgram previousProgram, RexProgram nextProgram,
	    boolean indicator, ImmutableBitSet groupSet,
	    List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls);

	public List<AggregateCall> getAggCallList() {
		return aggCalls;
	}	

	public List<Pair<AggregateCall, String>> getNamedAggCalls() {
		final int offset = getGroupCount() + getIndicatorCount();
		return Pair.zip(aggCalls, Util.skip(getRowType().getFieldNames(), offset));
	}
	  
	public int getGroupCount() {
		return groupSet.cardinality();
	}
	
	public int getIndicatorCount() {
		return indicator ? getGroupCount() : 0;
	}

	public ImmutableBitSet getGroupSet() {
		return groupSet;
	}

	public ImmutableList<ImmutableBitSet> getGroupSets() {
		return groupSets;
	}
	
	public RexProgram getPreviousProgram() {
	    return previousProgram;
	}
	
	public RexProgram getNextProgram() {
	    return nextProgram;
	}
	
	@Override public double estimateRowCount(RelMetadataQuery mq) {
		RexProgram program = (nextProgram != null) ? nextProgram : previousProgram;
		return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
	}

	// fix it
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
    	RelMetadataQuery mq) {
    	double dRows = mq.getRowCount(this);
		RexProgram program = (nextProgram != null) ? nextProgram : previousProgram;
    	double dCpu = mq.getRowCount(getInput())
    		* program.getExprCount();
    	double dIo = 0;
    	return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }
    
}
