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
	
		  RelOptCost previousLeftCost = planner.getCost(this.left, mq);
		  RelOptCost previousRightCost = planner.getCost(this.right, mq);
		  double rowCount = mq.getRowCount(this); 
		  double selectivity = mq.getSelectivity(this.left, this.getCondition()); //fix it
		  //System.out.println("selectivity:" + selectivity );
		  double leftRate = ((SaberCostBase) previousLeftCost/*mq.getCumulativeCost(this.left)*/).getRate();
		  double rightRate = ((SaberCostBase) previousRightCost).getRate();
		  double leftWindow = ((SaberCostBase) previousLeftCost).getWindow();
		  double rightWindow = ((SaberCostBase) previousRightCost).getWindow();
		  
		  double rate = selectivity * (leftRate*rightWindow + rightRate*leftWindow);
		  double cpuCost = SaberCostBase.Cj * (leftRate + rightRate);
		  double memory =  leftWindow + rightWindow;
		  double window =  selectivity * leftWindow * rightWindow;
		  window = (window < 1) ? 1 : window; // fix window size in order 
		  double R = (((SaberCostBase) previousLeftCost).getCpu() + ((SaberCostBase) previousRightCost).getCpu() + cpuCost) / rate;
		  
		  SaberCostFactory costFactory = (SaberCostFactory)planner.getCostFactory();
	      return costFactory.makeCost(rowCount, cpuCost, 0, rate, memory, window, R);
	}
}
