package calcite.planner.common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import calcite.cost.SaberCostBase.SaberCostFactory;

public class SaberTableScanRelBase extends TableScan implements SaberRelNode {

  //protected final SaberSQLExternalTable saberSQLExternalTable;

  protected SaberTableScanRelBase(RelOptCluster cluster,
                                  RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, table);
    //this.saberSQLExternalTable = table.unwrap(SaberSQLExternalTable.class);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {	      
	double rowCount = mq.getRowCount(this); //this is the rate
	//double cpuCost = SaberCostBase.Cj * rowCount;
	double window = 1;
	SaberCostFactory costFactory = (SaberCostFactory)planner.getCostFactory();
	return costFactory.makeCost(rowCount, 0, 0, rowCount, 0, window, 0);
  }  
  
}
