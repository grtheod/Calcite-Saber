package calcite.utils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

public class STable implements PreparingTable {
    protected final SCatalogReader catalogReader;
    private final boolean stream;
    private final double rowCount;
    protected final List<Map.Entry<String, RelDataType>> columnList =
        new ArrayList<>();
    protected RelDataType rowType;
    private List<RelCollation> collationList;
    protected final List<String> names;
    private final Set<String> monotonicColumnSet = Sets.newHashSet();
    private StructKind kind = StructKind.FULLY_QUALIFIED;

    public STable(SCatalogReader catalogReader, String catalogName,
        String schemaName, String name, boolean stream, double rowCount) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.rowCount = rowCount;
      this.names = ImmutableList.of(catalogName, schemaName, name);
    }

    public static STable create(SCatalogReader catalogReader,
        SSchema schema, String name, boolean stream, double rowCount) {
      STable table =
          new STable(catalogReader, schema.getCatalogName(), schema.name,
              name, stream, rowCount);
      schema.addTable(name);
      return table;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isAssignableFrom(Table.class)) {
        return clazz.cast(
            new JdbcTest.AbstractModifiableTable(Util.last(names)) {
              public RelDataType
              getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.createStructType(rowType.getFieldList());
              }

              public Collection getModifiableCollection() {
                return null;
              }

              public <E> Queryable<E>
              asQueryable(QueryProvider queryProvider, SchemaPlus schema,
                  String tableName) {
                return null;
              }

              public Type getElementType() {
                return null;
              }

              public Expression getExpression(SchemaPlus schema,
                  String tableName, Class clazz) {
                return null;
              }
            });
      }
      return null;
    }

    public double getRowCount() {
      return rowCount;
    }

    public RelOptSchema getRelOptSchema() {
      return catalogReader;
    }

    public RelNode toRel(ToRelContext context) {
      return LogicalTableScan.create(context.getCluster(), this);
    }

    public List<RelCollation> getCollationList() {
      return collationList;
    }

    public RelDistribution getDistribution() {
      return RelDistributions.BROADCAST_DISTRIBUTED;
    }

    public boolean isKey(ImmutableBitSet columns) {
      return false;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public boolean supportsModality(SqlModality modality) {
      return modality == (stream ? SqlModality.STREAM : SqlModality.RELATION);
    }

    public void onRegister(RelDataTypeFactory typeFactory) {
      rowType = typeFactory.createStructType(kind, Pair.right(columnList),
          Pair.left(columnList));
      collationList = deduceMonotonicity(this);
    }

    public List<String> getQualifiedName() {
      return names;
    }

    public SqlMonotonicity getMonotonicity(String columnName) {
      return monotonicColumnSet.contains(columnName)
          ? SqlMonotonicity.INCREASING
          : SqlMonotonicity.NOT_MONOTONIC;
    }

    public SqlAccessType getAllowedAccess() {
      return SqlAccessType.ALL;
    }

    public Expression getExpression(Class clazz) {
      throw new UnsupportedOperationException();
    }

    public void addColumn(String name, RelDataType type) {
      columnList.add(Pair.of(name, type));
    }

    public void addMonotonic(String name) {
      monotonicColumnSet.add(name);
      assert Pair.left(columnList).contains(name);
    }

    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      final STable table = new STable(catalogReader, names.get(0),
          names.get(1), names.get(2), stream, rowCount);
      table.columnList.addAll(columnList);
      table.columnList.addAll(extendedFields);
      table.onRegister(catalogReader.typeFactory);
      return table;
    }

    public void setKind(StructKind kind) {
      this.kind = kind;
    }

    public StructKind getKind() {
      return kind;
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
}
