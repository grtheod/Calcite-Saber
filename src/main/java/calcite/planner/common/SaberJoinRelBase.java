package calcite.planner.common;

import java.util.Collections;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberCostBase.SaberCostFactory;
import calcite.planner.logical.SaberRel;

public abstract class SaberJoinRelBase extends Join implements SaberRel {

	public SaberJoinRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
		      JoinRelType joinType){
		super(cluster, traits, left, right, condition, joinType, Collections.<String> emptySet());
	}
	
	@Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {		
	
	      double rowCount = mq.getRowCount(this); 
		  double selectivity = mq.getSelectivity(this.left, this.getCondition()); //fix it
		  //System.out.println("selectivity:" + selectivity );
		  double leftRate = ((SaberCostBase) mq.getCumulativeCost(this.left)).getRate();
		  double rightRate = ((SaberCostBase) mq.getCumulativeCost(this.right)).getRate();
		  double leftWindow = ((SaberCostBase) mq.getCumulativeCost(this.left)).getWindow();
		  double rightWindow = ((SaberCostBase) mq.getCumulativeCost(this.right)).getWindow();
		  
		  double rate = selectivity * (leftRate*rightWindow + rightRate*leftWindow);
		  double cpuCost = SaberCostBase.Cj * (leftRate + rightRate);
		  double memory =  leftWindow + rightWindow;
		  double window =  selectivity * leftWindow * rightWindow;
		  window = (window < 1) ? 1 : window; // fix window size in order 
		  double R = (((SaberCostBase) mq.getCumulativeCost(this.left)).getCpu() + ((SaberCostBase) mq.getCumulativeCost(this.right)).getCpu() + cpuCost) / rate;
		  
		  SaberCostFactory costFactory = (SaberCostFactory)planner.getCostFactory();
	      return costFactory.makeCost(rowCount, cpuCost, 0, rate, memory, window, R);
	}
}
