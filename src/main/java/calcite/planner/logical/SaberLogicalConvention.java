package calcite.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

import calcite.planner.physical.SaberRel;

public class SaberLogicalConvention implements Convention {	  

	public static final RelTrait INSTANCE = null;

	public Class getInterface() {
	    return SaberRel.class;
	  }

	  public String getName() {
	    return "SABER_LOGICAL";
	  }

	  public RelTraitDef getTraitDef() {
	    return ConventionTraitDef.INSTANCE;
	  }

	  public boolean satisfies(RelTrait trait) {
	    return this == trait;
	  }

	  public void register(RelOptPlanner planner) {}


	  public String toString() {
	    return getName();
	  }

	  @Override
	  public boolean canConvertConvention(Convention toConvention) {
		  // TODO Auto-generated method stub
		  return false;
	  }

	  @Override
	  public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
		  // TODO Auto-generated method stub
		  return false;
	  }	
}
