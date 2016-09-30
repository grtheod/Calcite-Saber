package calcite.planner;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;

//To implement saber type system, create a derived class of RelDataTypeSystemImpl and override values as needed.
public class SaberRelDataTypeSystem extends RelDataTypeSystemImpl {

	  public static final RelDataTypeSystem SABER_REL_DATATYPE_SYSTEM = new SaberRelDataTypeSystem();

	  public int getMaxNumericScale() {
	    return 38;
	  }

	  public int getMaxNumericPrecision() {
	    return 38;
	  }
}