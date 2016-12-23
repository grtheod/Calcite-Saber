package calcite.planner.logical;

import org.apache.calcite.plan.Convention;

import calcite.planner.common.SaberRelNode;

public interface SaberRel extends SaberRelNode {

  public static final Convention SABER_LOGICAL = new Convention.Impl("LOGICAL", SaberRel.class);

  //void physicalPlan(PhysicalPlanCreator physicalPlanCreator) throws Exception;

  <T> T accept(SaberRelVisitor<T> visitor);

  public static interface SaberRelVisitor<T> {
    T visit(SaberRel saberRel);
  }	
	
}
