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
}
