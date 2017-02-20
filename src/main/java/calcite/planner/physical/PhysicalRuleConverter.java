package calcite.planner.physical;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;

public class PhysicalRuleConverter {
	
	private final static Logger log = LogManager.getLogger (PhysicalRuleConverter.class);
	
	private RelNode logicalPlan;
	private Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> tablesMap;
	private SystemConf systemConf;
	private Map<Integer,ChainOfRules> chains;
	private Set<Query> queries = new HashSet<Query>();
	List <SaberRule> aggregates = new ArrayList <SaberRule>();
	long timestampReference;
	int queryId;
	int batchSize;
	
	public PhysicalRuleConverter (RelNode logicalPlan, Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> tablesMap , SystemConf systemConf, long timestampReference, int batchSize) {		
		this.logicalPlan = logicalPlan;
		this.tablesMap = tablesMap;
		this.systemConf = systemConf;
		this.chains = new HashMap<Integer,ChainOfRules>();
		this.timestampReference = timestampReference;
		this.queryId = 0;
		this.batchSize = batchSize;
	}
	
	public void convert(RelNode logicalPlan) {
		log.info("Convert logical plan");
		log.info(String.format("Root is %s", logicalPlan.toString()));
		
		List<RelNode> inputs = logicalPlan.getInputs();
		log.info(String.format("Root has %d inputs", inputs.size()));				
		if (inputs.size() == 0) {			
			convertSingleRelNode(logicalPlan);
		} else {
			convertMultipleRelNodes(logicalPlan);
		}
		
	}
	
	//Degenerate case : simple TableScan RelNode (select * from table).
	public void convertSingleRelNode(RelNode chainTail) {		
		log.info(String.format("Node %2d is %s", chainTail.getId(), chainTail.toString()));
		System.out.println();
		System.out.println(chainTail.getRelTypeName());
		System.out.println(chainTail.getTable().getQualifiedName());
		String tableKey = chainTail.getTable().getQualifiedName().toString().replace("[", "").replace("]", "").replace(", ", ".");
		Pair<ITupleSchema,Pair<byte [],ByteBuffer>> pair = tablesMap.get(tableKey);
		queryId = 0;
		RuleAssembler operation = new RuleAssembler(chainTail.getRelTypeName(), pair.left, queryId, timestampReference, true, batchSize);	    
		SaberRule rule = operation.construct();
		Query query = rule.getQuery();
		chains.put(logicalPlan.getId(), new ChainOfRules(query,rule.getOutputSchema(),rule.getWindow(),pair.right.left,false,true,0,0));
		System.out.println("Current query id : "+ query.getId());
		queries.add(query); 		    		    		   		    
		System.out.println("OutputSchema : " + rule.getOutputSchema().getSchema());
	}
	
	//General case.
	public Pair<Integer, String> convertMultipleRelNodes(RelNode logicalPlan) {
		/*
		 * Finds a chain of operators that finishes at the current root.
		 * 
		 * If children.size() == 2 (and no greater), then when the chain
		 * is broken `chainTail` node should be a join operator. 
		 * 
		 * The physical operators are constructed recursively. 
		 * 1)If children.size() == 0, we have a leaf node that is a simple LogicalTableScan.
		 * 2)If children.size() == 1, we have one or more (nested) children. In order to
		 * 	construct this operator, we have to construct its children first.
		 * 3)If children.size() == 2, we have a join. In order to built a join, we create
		 * 	recursively its left and right side children.
		 */		
		RelNode chainTail = logicalPlan;
			
		log.info(String.format("Node %2d is %s", chainTail.getId(), chainTail.toString()));
		
		List<RelNode> children = chainTail.getInputs();
		if (children.size() == 0){	//logicalTableScan operator
			System.out.println();
			System.out.println(chainTail.getRelTypeName());
			System.out.println(chainTail.getTable().getQualifiedName());
			String tableKey = chainTail.getTable().getQualifiedName().toString().replace("[", "").replace("]", "").replace(", ", ".");
			Pair<ITupleSchema,Pair<byte [],ByteBuffer>> pair = tablesMap.get(tableKey);
			RuleAssembler operation = new RuleAssembler(chainTail.getRelTypeName(), pair.left, queryId, timestampReference, false, batchSize);
													 //(chainTail.getRelTypeName(), null, pair.left, chainTail.getId(), timestampReference, null);	    
			SaberRule rule = operation.construct();
			Query query = rule.getQuery();
			ITupleSchema outputSchema = rule.getOutputSchema();
			chains.put(chainTail.getId(), new ChainOfRules(query,outputSchema,rule.getWindow(),pair.right.left,false,false,0,0));
			System.out.println("OutputSchema : " + outputSchema.getSchema());
			
			return new Pair<Integer, String>(chainTail.getId(),chainTail.getRelTypeName());			
		} else
		if (children.size() == 1) { //aggregate,window, filter or project operator
			chainTail = children.get(0);
			Pair <Integer, String> node = convertMultipleRelNodes(chainTail);
			
			System.out.println("-------------------------------------------");
			//System.out.println(logicalPlan.getRelTypeName());
			
			ChainOfRules chain = chains.get(node.left);
			RuleAssembler operation = new RuleAssembler(logicalPlan.getRelTypeName(), logicalPlan, chain.getOutputSchema(), queryId, timestampReference, chain.getWindow(), chain.getWindowOffset(), chain.getWindowBarrier(), batchSize);
			SaberRule rule = operation.construct();
		    
			if ((logicalPlan.getRelTypeName().equals("LogicalAggregate")) || (logicalPlan.getRelTypeName().equals("LogicalWindow"))) {
				aggregates.add(rule);
			}
		    
			Query query = rule.getQuery();
			System.out.println("Current query id : "+ query.getId());
			queryId++; //increment the queryId for the next query
			if (!(node.right.equals("LogicalTableScan"))) {
			    chain.getQuery().connectTo(query);
			    chains.put(logicalPlan.getId(), new ChainOfRules(query,rule.getOutputSchema(),rule.getWindow(),chain.getData(),false,false,rule.getWindowOffset().left,rule.getWindowOffset().right));
			} else { 
				//the first operator after LogicalTableScan is assigned to process the input data
				chains.put(logicalPlan.getId(), new ChainOfRules(query,rule.getOutputSchema(),rule.getWindow(),chain.getData(),false,true,rule.getWindowOffset().left,rule.getWindowOffset().right));
			}
			queries.add(query); 		    		    		   		    
			System.out.println("OutputSchema : " + rule.getOutputSchema().getSchema());
			
			return new Pair<Integer, String>(logicalPlan.getId(),logicalPlan.getRelTypeName());
		} else
		if (children.size() == 2) { //join operator
			
			System.out.println("-------------------------------------------");
			/*Build left side of join*/
			chainTail = children.get(0);
			System.out.println(chainTail);
			Pair <Integer, String> leftNode = convertMultipleRelNodes(chainTail);			
			ChainOfRules leftChain = chains.get(leftNode.left);
			
			System.out.println("-------------------------------------------");
			/*Build right side of join*/
			chainTail = children.get(1);
			System.out.println(chainTail);
			Pair <Integer, String> rightNode = convertMultipleRelNodes(chainTail);			
			ChainOfRules rightChain = chains.get(rightNode.left);
			
			String args = logicalPlan.getChildExps().toString();
			RuleAssembler operation = new RuleAssembler(logicalPlan.getRelTypeName(), logicalPlan, leftChain.getOutputSchema(), rightChain.getOutputSchema(), queryId, timestampReference, leftChain.getWindow(), rightChain.getWindow(), batchSize);
			SaberRule rule = operation.construct();
			Query query = rule.getQuery();
			System.out.println("Current query id : "+ query.getId());
			queryId++; //increment the queryId for the next query
			if ((!(leftNode.right.equals("LogicalTableScan"))) && (!(rightNode.right.equals("LogicalTableScan")))) {
			    leftChain.getQuery().connectTo(query);
			    rightChain.getQuery().connectTo(query);
			    chains.put(logicalPlan.getId(), new ChainOfRules(query,rule.getOutputSchema(),rule.getWindow(),leftChain.getData(),rightChain.getData(),true,false,false));
			} else
			if	((!(leftNode.right.equals("LogicalTableScan"))) && ((rightNode.right.equals("LogicalTableScan")))) {
				leftChain.getQuery().connectTo(query);
				chains.put(logicalPlan.getId(), new ChainOfRules(query,rule.getOutputSchema(), rule.getWindow(),rightChain.getData() , leftChain.getData(),true,true,false));
			} else
			if	(((leftNode.right.equals("LogicalTableScan"))) && (!(rightNode.right.equals("LogicalTableScan")))) {
				rightChain.getQuery().connectTo(query);
				chains.put(logicalPlan.getId(), new ChainOfRules(query,rule.getOutputSchema(), rule.getWindow(),leftChain.getData(), rightChain.getData(),true,true,false));
			} else {
				chains.put(logicalPlan.getId(), new ChainOfRules(query,rule.getOutputSchema(), rule.getWindow(), leftChain.getData(), rightChain.getData(),true,true,true));
			}
			
			queries.add(query);		   		
			System.out.println("OutputSchema : " + rule.getOutputSchema().getSchema());
											
			return new Pair<Integer, String>(logicalPlan.getId(),logicalPlan.getRelTypeName());
		} else {
			log.info ("Broken chain");			
		}
		return null;
	}
	
	
	public void execute (boolean getThroughput) {
				
		System.out.println("-------------------------------------------");
		List<Integer> throughputList = new ArrayList<Integer>();
		if (getThroughput) {
			for (Map.Entry<Integer,ChainOfRules> c : chains.entrySet()){						
				if(c.getValue().getIsFirst()) {
					if(c.getValue().getFlag() == false) {
						throughputList.add(c.getValue().query.getId());							
					} else {
						/*the first case doesn't work.*/
						if(c.getValue().getHasMore() == false) {
							throughputList.add(c.getValue().query.getId());	
						} else {
							throughputList.add(c.getValue().query.getId());	
						}
					}
				}					
			}
			System.out.println(throughputList.toString());
		}
		
		QueryApplication application = new QueryApplication(queries, throughputList);		
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
				for (Map.Entry<Integer,ChainOfRules> c : chains.entrySet()){						
					if(c.getValue().getIsFirst()) {
						if(c.getValue().getFlag() == false) {
							application.processData (c.getValue().query.getId(),c.getValue().getData());							
						} else {
							/*the first case doesn't work.*/
							if(c.getValue().getHasMore() == false) {
								application.processSecondStream (c.getValue().query.getId() ,c.getValue().getData());
							} else {
								application.processFirstStream  (c.getValue().query.getId(), c.getValue().getData());
								application.processSecondStream (c.getValue().query.getId(), c.getValue().getData2());
							}
						}
					}					
				}
				Thread.sleep(1);
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
