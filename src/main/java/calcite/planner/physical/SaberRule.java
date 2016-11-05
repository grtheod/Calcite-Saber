package calcite.planner.physical;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;

public interface SaberRule {
	
	public ITupleSchema getOutputSchema();
	
	public Query getQuery();
	
	public IOperatorCode getCpuCode();
	
	public IOperatorCode getGpuCode();
	
	public WindowDefinition getWindow();
	
	public WindowDefinition getWindow2();
	
}
