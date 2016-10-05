package calcite.planner.physical.rules;

import java.util.ArrayList;
import java.util.List;

import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;

public class SaberScanRule implements SaberRule{
	public static final String usage = "usage: Dummy Scan";
	List<String> args = new ArrayList<>();
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	
	
	public SaberScanRule(ITupleSchema schema, List<String> args) {
		this.schema = schema;
		this.args = args;
	}

	public void prepareRule() {
		this.outputSchema = this.schema;
	}

	@Override
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}

	@Override
	public Query getQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IOperatorCode getCpuCode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IOperatorCode getGpuCode() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
