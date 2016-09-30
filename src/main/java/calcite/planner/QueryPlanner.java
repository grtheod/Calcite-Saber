package calcite.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import calcite.planner.physical.SaberLogicalConvention;

/**
 * A wrapper around Calcite query planner that is used to parse, validate and generate a physical plan from a streaming
 * SQL query.
 */
public class QueryPlanner {

  public static final int STREAM_RULES = 0;
  public static final int SABER_REL_CONVERSION_RULES = 1;

  private final Planner planner;
  private final HepPlanner hepPlanner;

  public QueryPlanner(SchemaPlus schema) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder()
            .setLex(Lex.MYSQL)
            .build())
        .defaultSchema(schema)
        .operatorTable(SqlStdOperatorTable.instance()) // TODO: Implement Saber specific operator table
        .traitDefs(traitDefs)
        .context(Contexts.EMPTY_CONTEXT)
        .ruleSets(SaberRuleSets.getRuleSets())
        .costFactory(null) //If null, use the default cost factory for that planner.
        .typeSystem(SaberRelDataTypeSystem.SABER_REL_DATATYPE_SYSTEM)
        .build();
    this.planner = Frameworks.getPlanner(config);

    // Create hep planner for optimizations.
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleClass(ReduceExpressionsRule.class);
    hepProgramBuilder.addRuleClass(FilterTableScanRule.class);
    hepProgramBuilder.addRuleClass(FilterProjectTransposeRule.class);
    hepProgramBuilder.addRuleClass(FilterAggregateTransposeRule.class);
    hepProgramBuilder.addRuleClass(FilterJoinRule.class);
    hepProgramBuilder.addRuleClass(JoinProjectTransposeRule.class);
    //hepProgramBuilder.addRuleClass(ProjectToWindowRule.class);
    hepProgramBuilder.addRuleClass(ProjectJoinTransposeRule.class);
    hepProgramBuilder.addRuleClass(ProjectTableScanRule.class);
    hepProgramBuilder.addRuleClass(ProjectFilterTransposeRule.class);
    
    /*maybe add addMatchOrder(HepMatchOrder.BOTTOM_UP)to HepPlanner and change
     final HepPlanner hepPlanner = new HepPlanner(hepProgram,null, noDag, null, RelOptCostImpl.FACTORY);
      	with the option to keep the graph a tree(noDAG=true) or allow DAG(noDAG=false).
     */

    this.hepPlanner = new HepPlanner(hepProgramBuilder.build());
    
    /*The order in which the rules within a collection will be attempted is
     arbitrary, so if more control is needed, use addRuleInstance instead.*/
    
    this.hepPlanner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
    this.hepPlanner.addRule(FilterJoinRule.FILTER_ON_JOIN);
    this.hepPlanner.addRule(FilterProjectTransposeRule.INSTANCE);
    this.hepPlanner.addRule(FilterAggregateTransposeRule.INSTANCE);
    this.hepPlanner.addRule(FilterTableScanRule.INSTANCE);
    //this.hepPlanner.addRule(ProjectToWindowRule.PROJECT);
    this.hepPlanner.addRule(ProjectJoinTransposeRule.INSTANCE);
    this.hepPlanner.addRule(JoinProjectTransposeRule.BOTH_PROJECT);
    this.hepPlanner.addRule(ProjectTableScanRule.INSTANCE);
    this.hepPlanner.addRule(ProjectFilterTransposeRule.INSTANCE);
	
  }

  /*public OperatorRouter getOperatorRouter(String query) throws Exception {
    SaberRel relNode = getPlan(query);
    PhysicalPlanCreator physicalPlanCreator =
        PhysicalPlanCreator.create(relNode.getCluster().getTypeFactory());

    relNode.physicalPlan(physicalPlanCreator);

    return physicalPlanCreator.getRouter();
  }

  public SamzaRel getPlan(String query) throws ValidationException, RelConversionException, SqlParseException {
    return (SamzaRel) validateAndConvert(planner.parse(query));
  }*/

  private RelNode validateAndConvert(SqlNode sqlNode) throws ValidationException, RelConversionException {
    SqlNode validated = validateNode(sqlNode);
    RelNode relNode = convertToRelNode(validated);

    // Drill does pre-processing too. We can do pre processing here if needed.
    // Drill also preserve the validated type of the sql query for later use. But current Calcite
    // API doesn't provide a way to get validated type.

    return convertToSaberRel(relNode);
  }

  private RelNode convertToSaberRel(RelNode relNode) throws RelConversionException {
    RelTraitSet traitSet = relNode.getTraitSet();
    traitSet = traitSet.simplify(); // TODO: Is this the correct thing to do? Why relnode has a composite trait?
    return planner.transform(SABER_REL_CONVERSION_RULES, traitSet.plus(SaberLogicalConvention.INSTANCE), relNode);
  }

  public RelNode convertToRelNode(SqlNode sqlNode) throws RelConversionException {
    final RelNode convertedNode = planner.convert(sqlNode);

    final RelMetadataProvider provider = convertedNode.getCluster().getMetadataProvider();

    // Register RelMetadataProvider with HepPlanner.
    final List<RelMetadataProvider> list = Lists.newArrayList(provider);
    hepPlanner.registerMetadataProviders(list);
    final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
    convertedNode.accept(new MetaDataProviderModifier(cachingMetaDataProvider));
    
    /*
     * Maybe change the above two lines of code with :
     * RelMetadataProvider plannerChain = ChainedRelMetadataProvider.of(list);
     * convertedNode.getCluster().setMetadataProvider(plannerChain);
     */
    /*
    List<RelMetadataProvider> list = Lists.newArrayList();
    list.add(DefaultRelMetadataProvider.INSTANCE);
    hepPlanner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain =
        ChainedRelMetadataProvider.of(list);
    convertedNode.getCluster().setMetadataProvider(plannerChain);
    */
    
    System.out.println("Applying rules...");
    hepPlanner.setRoot(convertedNode);
    RelNode rel = hepPlanner.findBestExp();
    

    // I think this line reset the metadata provider instances changed for hep planner execution.
    rel.accept(new MetaDataProviderModifier(provider));
   
    return rel;
  }

  private RelNode applyStreamRules(RelNode relNode) throws RelConversionException {
    return planner.transform(STREAM_RULES, relNode.getTraitSet(), relNode);
  }

  private SqlNode validateNode(SqlNode sqlNode) throws ValidationException {
    SqlNode validatedSqlNode = planner.validate(sqlNode);

    validatedSqlNode.accept(new UnsupportedOperatorsVisitor());

    return validatedSqlNode;
  }


  public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }

    SqlNode validatedSqlNode = planner.validate(sqlNode);
      
    RelNode beforeplan= convertToRelNode(validatedSqlNode);   
    //compute cost
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    //RelMetadataQuery.THREAD_PROVIDERS.set( JaninoRelMetadataProvider.of(new MyRelMetadataProvider()));
    /*RelOptCost relCost= mq.getCumulativeCost(beforeplan); //beforeplan.computeSelfCost( hepPlanner,mq);	 	   
    System.out.println("Plan cost is : " + relCost.toString());

    System.out.println("Row count : " + mq.getRowCount(beforeplan));*/
    System.out.println("Returning plan...");
    return beforeplan;    
    //return planner.rel(validatedSqlNode).project();
  }  
  
  
  public SqlNode parseQuery(String sql) throws Exception {
      SqlParser parser = SqlParser.create(sql);
      return parser.parseQuery();
  }
  
  
  // TODO: This is from Drill. Not sure what it does.
  public static class MetaDataProviderModifier extends RelShuttleImpl {
    private final RelMetadataProvider metadataProvider;

    public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
      this.metadataProvider = metadataProvider;
    }

    @Override
    public RelNode visit(TableScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(LogicalValues values) {
      values.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(values);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      child.accept(this);
      parent.getCluster().setMetadataProvider(metadataProvider);
      return parent;
    }
  }

}