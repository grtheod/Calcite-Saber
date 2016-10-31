package calcite.cost;

import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

import com.google.common.collect.ImmutableList;

public class SaberDefaultRelMetadataProvider {
	private SaberDefaultRelMetadataProvider() {
	}

	public static final RelMetadataProvider INSTANCE = ChainedRelMetadataProvider.of(ImmutableList
		.of(SaberRelMdRowCount.SOURCE,
	        SaberRelMdDistinctRowCount.SOURCE,
	        DefaultRelMetadataProvider.INSTANCE));
}
