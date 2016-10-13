package calcite.planner.physical;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import calcite.utils.SaberSchema;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;

public class PhysicalRuleConverter {
	
	private final static Logger log = LogManager.getLogger (PhysicalRuleConverter.class);
	
	private RelNode logicalPlan;
	private Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> tablesMap;
	private SystemConf systemConf;
	
	public PhysicalRuleConverter (RelNode logicalPlan, Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> tablesMap , SystemConf systemConf) {
		
		this.logicalPlan = logicalPlan;
		this.tablesMap = tablesMap;
		this.systemConf = systemConf;
	}
	
	public void convert () {
		
		log.info("Convert logical plan");
		log.info(String.format("Root is %s", logicalPlan.toString()));
		
		List<RelNode> inputs = logicalPlan.getInputs();
		log.info(String.format("Root has %d inputs", inputs.size()));
		
		/*
		 * Finds a chain of operators that finishes at the current root.
		 * 
		 * If children.size() == 2 (and no greater), then when the chain
		 * is broken `chainTail` node should be a join operator. 
		 */
		
		RelNode chainTail = logicalPlan;
		while (true) {
			
			log.info(String.format("Node %2d is %s", chainTail.getId(), chainTail.toString()));
			
			List<RelNode> children = chainTail.getInputs();
			if (children.size() > 0 && children.size() < 2)
				chainTail = children.get(0);
			else {
				log.info ("Broken chain");
				break;
			}
		}
	}
	
	public void execute () {
		
		/* Create a list of logical operators for a given plan*/
		String operators[] = RelOptUtil.toString(logicalPlan).split("\\r?\\n");
						
		List<Pair<String,List<String>>> physicalOperators = new ArrayList<Pair<String,List<String>>>();
		String schema = "",table = "",temp,operator,logicalOperator,operands;
		int whiteSpaces; /* whitespaces from the begging of the logical rule will be used for nested joins*/
		for (int counter=operators.length - 1; counter >= 0;counter--){
			
			operator = operators[counter];
			List <String> args = new ArrayList<String>(); 
			//System.out.println("Converting operator : "+ operator);
			whiteSpaces = 0;
			
			if ((operator).contains("LogicalTableScan")){
				temp = operator.substring(0,operator.indexOf("L"));
				whiteSpaces = temp.length();
				
				logicalOperator = (operator.substring(0,operator.indexOf("("))).trim();
				temp = operator.substring(operator.indexOf('[')+2,operator.indexOf(']'));
				String temps[] = temp.split(", ");
				schema = temps[0];
				table = temps[1];
            			args.add("--schema");
            			args.add(schema);
            			args.add("--table");
            			args.add(table);
			} else{
				temp = operator.substring(0,operator.indexOf("L"));
				whiteSpaces = temp.length();
				
				logicalOperator = (operator.substring(0,operator.indexOf("("))).trim();
				operands = operator.substring(operator.indexOf('('));
				args.add("--operands");
				args.add(operands);
			}
			args.add("--whitespaces");
			args.add(Integer.toString(whiteSpaces));
			physicalOperators.add(new Pair<String, List<String>>(logicalOperator,args));
			//System.out.println("WhiteSpaces = "+ whiteSpaces);
		}
		
		System.out.println("---------------------------------------------");
	    	/* Transformation from relational operators to physical => */ 
				
		/*  Creating a single chain of queries. For complex queries that use JOIN
		 *  we have to create multiple chains and join them. */	    
		RuleAssembler operation;
		SaberRule rule;
		Query query = null;
		ITupleSchema outputSchema = null;
		Set<Query> queries = new HashSet<Query>();
		int queryId = 0;
		long timestampReference = System.nanoTime();
		List <SaberRule> aggregates = new ArrayList <SaberRule>();
		
		
		Map<Integer, List <ChainOfRules>> chainMap = new HashMap<Integer, List <ChainOfRules>>();
		List <ChainOfRules> chains = new ArrayList <ChainOfRules>();
		ChainOfRules chain = null;
		for(Pair<String,List<String>> po : physicalOperators){
			po.right.add("--queryId");
			po.right.add(Integer.toString(queryId));
			po.right.add("--timestampReference");
			po.right.add(Long.toString(timestampReference));

			if (po.left.equals("LogicalTableScan")){
				String tableKey = po.right.get(po.right.indexOf("--schema") +1) + 
						"." + po.right.get(po.right.indexOf("--table") +1);
				Pair<ITupleSchema,Pair<byte [],ByteBuffer>> pair = tablesMap.get(tableKey);
				operation = new RuleAssembler(po.left, po.right, pair.left);	    
			    rule = operation.construct();
			    query = rule.getQuery();
			    outputSchema = rule.getOutputSchema();			    
				queryId--;

				if (!(chain == null)){
					chains.add(chain);
				}
				int wS = Integer.parseInt(po.right.get( po.right.indexOf("--whitespaces") +1));
				chain = new ChainOfRules(wS,query,outputSchema,pair.right.left,false);
			} else
			if (po.left.equals("LogicalJoin")) {
				
				/*
				 * Not implemented yet;
				 * */
			    operation = new RuleAssembler(po.left, po.right, null);
			    rule = operation.construct();
			    
			    
			} else					
			if (query==null) {
			    ITupleSchema inputSchema = outputSchema;
			    operation = new RuleAssembler(po.left, po.right, inputSchema);
			    rule = operation.construct();
			    query = rule.getQuery();
			    outputSchema = rule.getOutputSchema();
			    queries.add(query);
			    
			    int wS = Integer.parseInt(po.right.get( po.right.indexOf("--whitespaces") +1));
			    chain.addRule(wS, query, outputSchema);
			} else {
			    operation = new RuleAssembler(po.left, po.right, outputSchema);	    
			    rule = operation.construct();
			    Query query1 = rule.getQuery();
			    outputSchema = rule.getOutputSchema();
			    query.connectTo(query1);
			    queries.add(query1);
			    query = query1; //keep the last query to build the chain
			    
			    int wS = Integer.parseInt(po.right.get( po.right.indexOf("--whitespaces") +1));
			    chain.addRule(wS, query, outputSchema);
			}
			
			if (po.left.equals("LogicalAggregate")){
			    //aggregates.add(rule);
			}
			queryId++;
		    	System.out.println("OutputSchema : " + outputSchema.getSchema());
		    if (!(po.left.equals("LogicalTableScan"))){
		    	System.out.println("Query id : "+ query.getName());
		    }
		    System.out.println();
		}
		chains.add(chain);
				
		QueryApplication application = new QueryApplication(queries);		
		application.setup();
		
		/* The path is query -> dispatcher -> handler -> aggregator */
		/* I am not sure of this : */
		for ( SaberRule agg : aggregates){
			if (systemConf.CPU)
				agg.getQuery().setAggregateOperator((IAggregateOperator) agg.getCpuCode());
			else
				agg.getQuery().setAggregateOperator((IAggregateOperator) agg.getGpuCode());
		}
		
		/* Execute the query. */	
		if (systemConf.LATENCY_ON) {
			long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* usec */
			//long packedTimestamp = Utils.pack(systemTimestamp, b.getLong(0));
			//b.putLong(0, packedTimestamp);
		}
		
		try {
			while (true) {
				for (ChainOfRules c : chains){
					if(c.getFlag() == false) {
						application.processData (c.getData());
					} else {
						//execute join =>
						//application.processFirstStream  (data1);
						//application.processSecondStream (data2);
					}
					
				}
				if (systemConf.LATENCY_ON){
					//b.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), 1L));
				}
			}
		} catch (Exception e) { 
			e.printStackTrace(); 
			System.exit(1);
		}		
	}
}
