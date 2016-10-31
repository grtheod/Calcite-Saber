package calcite.cost;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

public class SaberRelMdDistinctRowCount extends RelMdDistinctRowCount{
	private static final SaberRelMdDistinctRowCount INSTANCE =
		new SaberRelMdDistinctRowCount();

	public static final RelMetadataProvider SOURCE =
		ReflectiveRelMetadataProvider.reflectiveSource(
			BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

	public Double getDistinctRowCount(RelNode rel, ImmutableBitSet groupKey, RexNode predicate) {
		if (rel instanceof TableScan) {
			return getDistinctRowCount(rel, groupKey, predicate);
		} else {
		    return super.getDistinctRowCount(rel, null, groupKey, predicate); //add RelMetadataQuery
		}
	}

	private Double getDistinctRowCount(TableScan scan, ImmutableBitSet groupKey, RexNode predicate) {
		// Consistent with the estimation of Aggregate row count in RelMdRowCount : distinctRowCount = rowCount * 10%.
		return scan.getRows() * 0.1;
	}
}