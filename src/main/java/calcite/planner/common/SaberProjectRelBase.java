package calcite.planner.common;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberCostBase.SaberCostFactory;
import calcite.planner.physical.ExpressionBuilder;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;

public abstract class SaberProjectRelBase extends Project implements SaberRelNode {
	
	protected SaberProjectRelBase(RelOptCluster cluster, RelTraitSet traits,
              RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
		super(cluster, traits, input, projects, rowType);
	}

	@Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {		

      double rowCount = mq.getRowCount(this); 
      double rate = ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getRate();
      double cpuCost = SaberCostBase.Cs * rate;
      double window =  ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getWindow();
	  List<RexNode> projectedAttrs = this.getChildExps(); 
	  double windowRange = 0; // find it in a better way
	  for (RexNode attr : projectedAttrs){
		  if (!(attr.getKind().toString().equals("INPUT_REF"))) {
				int tempSize = new ExpressionBuilder(attr,0).getWindowForPlan();			
				if (tempSize > 0) {
					windowRange = tempSize * rate; // W = T * λi
				}
		  }
	  }
	  window = (windowRange > 0) ? windowRange : window; 
	  double R = ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getCpu() / rate;
	  
      SaberCostFactory costFactory = (SaberCostFactory)planner.getCostFactory();
      return costFactory.makeCost(rowCount, cpuCost, 0, rate, 0, window, R);
    }
	
}
