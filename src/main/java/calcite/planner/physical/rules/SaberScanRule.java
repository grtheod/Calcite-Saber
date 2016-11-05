package calcite.planner.physical.rules;

import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;

public class SaberScanRule implements SaberRule {
	public static final String usage = "usage: Dummy Scan";
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;	
	
	public SaberScanRule(ITupleSchema schema) {
		this.schema = schema;		
	}

	public void prepareRule() {
		this.outputSchema = this.schema;
	}

	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}

	public Query getQuery() {
		return null;
	}

	public IOperatorCode getCpuCode() {
		return null;
	}

	public IOperatorCode getGpuCode() {
		return null;
	}

	public WindowDefinition getWindow() {
		return null;
	}

	public WindowDefinition getWindow2() {
		return null;
	}
	
}
