package calcite.planner.physical;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import calcite.utils.SaberSchema;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;

public class PhysicalRuleConverter {
	
	RelNode logicalPlan;
	
	public PhysicalRuleConverter(RelNode logicalPlan){
		this.logicalPlan=logicalPlan;
	}
	
	public void execute() {
		
		/* Create a list of logical operators for a given plan*/
		String operators[] = RelOptUtil.toString(logicalPlan).split("\\r?\\n");
						
		List<Pair<String,List<String>>> physicalOperators = new ArrayList<Pair<String,List<String>>>();
		String schema = "",table = "",temp,operator,logicalOperator,operands;
		for (int counter=operators.length - 1; counter >= 0;counter--){
			
			operator = operators[counter];
			List <String> args = new ArrayList<String>(); 
			//System.out.println("Converting operator : "+ operator);
			
			if ((operator).contains("LogicalTableScan")){
				temp = operator.substring(operator.indexOf('[')+2,operator.indexOf(']'));
				String temps[] = temp.split(", ");
				schema = temps[0];
				table = temps[1];
			} else{
				logicalOperator = (operator.substring(0,operator.indexOf("("))).trim();
				operands = operator.substring(operator.indexOf('('));
				if (!(schema.equals("") && table.equals(""))) {				
	                args.add("--schema");
	                args.add(schema);
	                args.add("--table");
					args.add(table);
	                table="";
					schema="";
				} 
				args.add("--operands");
				args.add(operands);
				physicalOperators.add(new Pair<String, List<String>>(logicalOperator,args));
			}
		}
		
		System.out.println("---------------------------------------------");
	    /* Transformation from relational operators to physical => */ 
		
		/* Setting up the input schema and the input data of Saber */
	    SaberSchema s = new SaberSchema(6);
	    ITupleSchema orders = s.createTable(); //column references have +1 value !!
	    orders.setAttributeName(1, "orderid");
	    orders.setAttributeName(2, "productid");
	    orders.setAttributeName(3, "untis");
	   
	    Pair<byte [],ByteBuffer> mockData = s.fillTable(orders);
	    byte [] data = mockData.left;
	    ByteBuffer b = mockData.right; 
	    
		/*  Creating a single chain of queries. For complex queries that use JOIN
		 *  we have to create multiple chains and join them. */	    
		RuleAssembler operation;
		SaberRule rule;
		Query query = null;
		ITupleSchema outputSchema = null;
		Set<Query> queries = new HashSet<Query>();
		int i = 0;
		List <SaberRule> aggregates = new ArrayList <SaberRule>();
		for(Pair<String,List<String>> po : physicalOperators){
			if (i == 0) {
			    operation = new RuleAssembler(physicalOperators.get(0).left, physicalOperators.get(0).right, orders);
			    rule = operation.construct();
			    query = rule.getQuery();
			    outputSchema = rule.getOutputSchema();
			    System.out.println("OutputSchema : " + outputSchema.getSchema());
				queries.add(query);
			} else {
				operation = new RuleAssembler(physicalOperators.get(i).left, physicalOperators.get(i).right, outputSchema);	    
			    rule = operation.construct();
			    Query query1 = rule.getQuery();
			    outputSchema = rule.getOutputSchema();
			    System.out.println("OutputSchema : " + outputSchema.getSchema());			    
			    query.connectTo(query1);
			    queries.add(query1);
			    query = query1; //keep the last query to build the chain
			}
			if (physicalOperators.get(i).left.equals("LogicalAggregate")){
				aggregates.add(rule);
			}
			i++;
			System.out.println();
		}
		
				
		QueryApplication application = new QueryApplication(queries);		
		application.setup();
		
		/* The path is query -> dispatcher -> handler -> aggregator */
		/* I am not sure of this : */
		for ( SaberRule agg : aggregates){
			if (SystemConf.CPU)
				agg.getQuery().setAggregateOperator((IAggregateOperator) agg.getCpuCode());
			else
				agg.getQuery().setAggregateOperator((IAggregateOperator) agg.getGpuCode());
		}
		
		/* Execute the query. */
		SystemConf.LATENCY_ON = false;
		long timestampReference = System.nanoTime(); 
		/* I have used different timestamps for all the queries. Maybe they should have the same. */
		
		if (SystemConf.LATENCY_ON) {
			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
			long packedTimestamp = Utils.pack(systemTimestamp, b.getLong(0));
			b.putLong(0, packedTimestamp);
		}
		
		try {
			while (true) {	
				application.processData (data);
				if (SystemConf.LATENCY_ON)
					b.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), 1L));
			}
		} catch (Exception e) { 
			e.printStackTrace(); 
			System.exit(1);
		}		
	    
	}

	
}
