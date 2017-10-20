package calcite.cost;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

public class SaberRelMdRowCount extends RelMdRowCount{
	  private static final SaberRelMdRowCount INSTANCE = new SaberRelMdRowCount();

	  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, INSTANCE);

	  public Double getRowCount(Aggregate rel) {
	    ImmutableBitSet groupKey = ImmutableBitSet.range(rel.getGroupCount());

	    if (groupKey.isEmpty()) {
	      return 1.0;
	    } else {
	      return super.getRowCount(rel, null); //add RelMetadataQuery
	    }
	  }

	  public Double getRowCount(Filter rel) {
	    return rel.getRows();
	  }
	  /*
	  public Double getRowCount (RelSubset subset, RelMetadataQuery mq) {
		  if (!Bug.CALCITE_1048_FIXED)
			  return mq.getRowCount(Util.first(subset.getBest(), subset.getOriginal()));
		  Double v = null;
		  for (RelNode r : subset.getRels()) {
			  try {
				  v = NumberUtil.min(v, mq.getRowCount(r));
			  } catch (CyclicMetadataException e) {
				  
			  } catch (Throwable e) {
				  e.printStackTrace();
			  }
		  }
		  return Util.first(v, 1e6d);
	  }
	  */
}
