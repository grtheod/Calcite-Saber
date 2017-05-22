package calcite.planner.physical.rules.util;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;

public interface SaberRuleUtil {
	
	public void build();

	public ITupleSchema getOutputSchema();
	
	public QueryOperator getOperator();
	
	public IOperatorCode getCpuCode();
	
	public IOperatorCode getGpuCode();
	
	public WindowDefinition getWindow();
	
	public WindowDefinition getWindow2();
}
