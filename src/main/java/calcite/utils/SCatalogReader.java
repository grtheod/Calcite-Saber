package calcite.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.calcite.sql.type.ObjectSqlType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

public class SCatalogReader implements CatalogReader {

	  protected static final String DEFAULT_CATALOG = "CATALOG";
	  protected static final String DEFAULT_SCHEMA = "SALES";
	  
	  public static final Ordering<Iterable<String>>
	  CASE_INSENSITIVE_LIST_COMPARATOR =
	      Ordering.from(String.CASE_INSENSITIVE_ORDER).lexicographical();

	  protected final RelDataTypeFactory typeFactory;
	  private final boolean caseSensitive;
	  private final Map<List<String>, STable> tables;
	  protected final Map<String, SSchema> schemas;
	  private RelDataType addressType;
	  
	  public SCatalogReader(RelDataTypeFactory typeFactory,
		      boolean caseSensitive) {
		    this.typeFactory = typeFactory;
		    this.caseSensitive = caseSensitive;
		    if (caseSensitive) {
		      tables = Maps.newHashMap();
		      schemas = Maps.newHashMap();
		    } else {
		      tables = Maps.newTreeMap(CASE_INSENSITIVE_LIST_COMPARATOR);
		      schemas = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
		    }
	  }
	  
	  public SCatalogReader init() {
		    final RelDataType intType =
		        typeFactory.createSqlType(SqlTypeName.INTEGER);
		    final RelDataType intTypeNull =
		        typeFactory.createTypeWithNullability(intType, true);
		    final RelDataType varchar10Type =
		        typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
		    final RelDataType varchar20Type =
		        typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
		    final RelDataType timestampType =
		        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
		    final RelDataType dateType =
		        typeFactory.createSqlType(SqlTypeName.DATE);
		    final RelDataType booleanType =
		        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
		    
		    addressType =
		            new ObjectSqlType(
		                SqlTypeName.STRUCTURED,
		                new SqlIdentifier("ADDRESS", SqlParserPos.ZERO),
		                false,
		                Arrays.asList(
		                    new RelDataTypeFieldImpl("STREET", 0, varchar20Type),
		                    new RelDataTypeFieldImpl("CITY", 1, varchar20Type),
		                    new RelDataTypeFieldImpl("ZIP", 2, intType),
		                    new RelDataTypeFieldImpl("STATE", 3, varchar20Type)),
		                RelDataTypeComparability.NONE);		 
		    
		    SSchema salesSchema = new SSchema("SALES");
		    registerSchema(salesSchema); 
		    
		    final STable ordersTable =
		            STable.create(this, salesSchema, "ORDERS", false, 12);
		    ordersTable.addColumn("ORDERID", intType);
		    ordersTable.addColumn("PRODUCTID", intType);
		    ordersTable.addColumn("UNITS", intType);
		    registerTable(ordersTable);
		    
		    final STable productsTable =
		            STable.create(this, salesSchema, "PRODUCTS", false, 6);		    
		    productsTable.addColumn("PRODUCTID", intType);
		    productsTable.addColumn("DESCRIPTION", varchar10Type);
		    registerTable(productsTable);
		    
		    return this;		    
	  }
	  

	  public void lookupOperatorOverloads(SqlIdentifier opName,
	      SqlFunctionCategory category, SqlSyntax syntax,
	      List<SqlOperator> operatorList) {
	  }

	  public List<SqlOperator> getOperatorList() {
	    return ImmutableList.of();
	  }

	  public Prepare.CatalogReader withSchemaPath(List<String> schemaPath) {
	    return this;
	  }

	  public Prepare.PreparingTable getTableForMember(List<String> names) {
	    return getTable(names);
	  }

	  public RelDataTypeFactory getTypeFactory() {
	    return typeFactory;
	  }

	  public void registerRules(RelOptPlanner planner) {
	  }

	  protected void registerTable(STable table) {
	    table.onRegister(typeFactory);
	    tables.put(table.getQualifiedName(), table);
	  }

	  protected void registerSchema(SSchema schema) {
	    schemas.put(schema.name, schema);
	  }

	  public Prepare.PreparingTable getTable(final List<String> names) {
	    switch (names.size()) {
	    case 1:
	      // assume table in SALES schema (the original default)
	      // if it's not supplied, because SqlValidatorTest is effectively
	      // using SALES as its default schema.
	      return tables.get(
	          ImmutableList.of(DEFAULT_CATALOG, DEFAULT_SCHEMA, names.get(0)));
	    case 2:
	      return tables.get(
	          ImmutableList.of(DEFAULT_CATALOG, names.get(0), names.get(1)));
	    case 3:
	      return tables.get(names);
	    default:
	      return null;
	    }
	  }

	  public RelDataType getNamedType(SqlIdentifier typeName) {
	    if (typeName.equalsDeep(addressType.getSqlIdentifier(), Litmus.IGNORE)) {
	      return addressType;
	    } else {
	      return null;
	    }
	  }

	  public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
	    List<SqlMoniker> result;
	    switch (names.size()) {
	    case 0:
	      // looking for catalog and schema names
	      return ImmutableList.<SqlMoniker>builder()
	          .add(new SqlMonikerImpl(DEFAULT_CATALOG, SqlMonikerType.CATALOG))
	          .addAll(getAllSchemaObjectNames(ImmutableList.of(DEFAULT_CATALOG)))
	          .build();
	    case 1:
	      // looking for schema names
	      result = Lists.newArrayList();
	      for (SSchema schema : schemas.values()) {
	        final String catalogName = names.get(0);
	        if (schema.getCatalogName().equals(catalogName)) {
	          final ImmutableList<String> names1 =
	              ImmutableList.of(catalogName, schema.name);
	          result.add(new SqlMonikerImpl(names1, SqlMonikerType.SCHEMA));
	        }
	      }
	      return result;
	    case 2:
	      // looking for table names in the given schema
	      SSchema schema = schemas.get(names.get(1));
	      if (schema == null) {
	        return Collections.emptyList();
	      }
	      result = Lists.newArrayList();
	      for (String tableName : schema.tableNames) {
	        result.add(
	            new SqlMonikerImpl(
	                ImmutableList.of(schema.getCatalogName(), schema.name,
	                    tableName),
	                SqlMonikerType.TABLE));
	      }
	      return result;
	    default:
	      return Collections.emptyList();
	    }
	  }

	  public List<String> getSchemaName() {
	    return ImmutableList.of(DEFAULT_CATALOG, DEFAULT_SCHEMA);
	  }

	  public RelDataTypeField field(RelDataType rowType, String alias) {
	    return SqlValidatorUtil.lookupField(caseSensitive, rowType, alias);
	  }

	  public boolean matches(String string, String name) {
	    return Util.matches(caseSensitive, string, name);
	  }

	  public RelDataType createTypeFromProjection(final RelDataType type,
	      final List<String> columnNameList) {
	    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
	        typeFactory, caseSensitive);
	  }

	  private static List<RelCollation> deduceMonotonicity(
	      Prepare.PreparingTable table) {
	    final List<RelCollation> collationList = Lists.newArrayList();

	    // Deduce which fields the table is sorted on.
	    int i = -1;
	    for (RelDataTypeField field : table.getRowType().getFieldList()) {
	      ++i;
	      final SqlMonotonicity monotonicity =
	          table.getMonotonicity(field.getName());
	      if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
	        final RelFieldCollation.Direction direction =
	            monotonicity.isDecreasing()
	                ? RelFieldCollation.Direction.DESCENDING
	                : RelFieldCollation.Direction.ASCENDING;
	        collationList.add(
	            RelCollations.of(
	                new RelFieldCollation(i, direction)));
	      }
	    }
	    return collationList;
	  }

	  @Override public boolean isCaseSensitive() {
		    return caseSensitive;
	  }
  	
}
