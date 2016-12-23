package calcite.planner.common;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberCostBase.SaberCostFactory;

public abstract class SaberAggregateRelBase extends Aggregate implements SaberRelNode {
	
	protected SaberAggregateRelBase(RelOptCluster cluster, RelTraitSet traits,
	          RelNode child, boolean indicator, ImmutableBitSet groupSet,
	          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
		super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
	}
	
	@Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
	      
		double rowCount = mq.getRowCount(this); 
	    float multiplier = 1f + (float) aggCalls.size() * 0.125f;
	    for (AggregateCall aggCall : aggCalls) {
	      if (aggCall.getAggregation().getName().equals("SUM")) {
	        // Pretend that SUM costs a little bit more than $SUM0,
	        // to make things deterministic.
	        multiplier += 0.0125f;
	      }
	    }	  
	    //System.out.println("multiplier:" + multiplier );
	    double inputRate = ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getRate();
	    double outputRate = multiplier * inputRate;
	    double cpuCost = multiplier * SaberCostBase.Cs * inputRate;	    
		double window = ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getWindow();
		double memory = window;
		
		double R = (((SaberCostBase) mq.getCumulativeCost(this.getInput())).getCpu() + cpuCost) / outputRate;
		SaberCostFactory costFactory = (SaberCostFactory)planner.getCostFactory();
		return costFactory.makeCost(multiplier * rowCount, cpuCost, 0, outputRate, memory, window, R);
	}
}
