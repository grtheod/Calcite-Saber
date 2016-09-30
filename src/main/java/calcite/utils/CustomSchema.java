package calcite.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
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
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class CustomSchema extends AbstractSchema implements RelOptSchemaWithSampling {
	
    private final SqlValidatorCatalogReader catalogReader;
    private final RelDataTypeFactory typeFactory;

    public CustomSchema(
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      this.catalogReader = catalogReader;
      this.typeFactory = typeFactory;
    }
    
    public RelOptTable getTableForMember(List<String> names) {
      final SqlValidatorTable table = catalogReader.getTable(names);
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList = deduceMonotonicity(table);
      if (names.size() < 3) {
        String[] newNames2 = {"CATALOG", "SALES", ""};
        List<String> newNames = new ArrayList<>();
        int i = 0;
        while (newNames.size() < newNames2.length) {
          newNames.add(i, newNames2[i]);
          ++i;
        }
        names = newNames;
      }
      return createColumnSet(table, names, rowType, collationList);
    }

    private List<RelCollation> deduceMonotonicity(SqlValidatorTable table) {
      final RelDataType rowType = table.getRowType();
      final List<RelCollation> collationList = new ArrayList<>();

      // Deduce which fields the table is sorted on.
      int i = -1;
      for (RelDataTypeField field : rowType.getFieldList()) {
        ++i;
        final SqlMonotonicity monotonicity =
            table.getMonotonicity(field.getName());
        if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
          final RelFieldCollation.Direction direction =
              monotonicity.isDecreasing()
                  ? RelFieldCollation.Direction.DESCENDING
                  : RelFieldCollation.Direction.ASCENDING;
          collationList.add(
              RelCollations.of(new RelFieldCollation(i, direction)));
        }
      }
      return collationList;
    }
    
    public RelOptTable getTableForMember(
            List<String> names,
            final String datasetName,
            boolean[] usedDataset) {
          final RelOptTable table = getTableForMember(names);
		return table;    
    }
    
    protected CustomColumnSet createColumnSet(
        SqlValidatorTable table,
        List<String> names,
        final RelDataType rowType,
        final List<RelCollation> collationList) {
      return new CustomColumnSet(names, rowType, collationList);
    }

    public RelDataTypeFactory getTypeFactory() {
      return typeFactory;
    }

    public void registerRules(RelOptPlanner planner) throws Exception {
    }

    protected class CustomColumnSet implements RelOptTable {
        private final List<String> names;
        private final RelDataType rowType;
        private final List<RelCollation> collationList;

        protected CustomColumnSet(
            List<String> names,
            RelDataType rowType,
            final List<RelCollation> collationList) {
          this.names = ImmutableList.copyOf(names);
          this.rowType = rowType;
          this.collationList = collationList;
        }

        public <T> T unwrap(Class<T> clazz) {
          if (clazz.isInstance(this)) {
            return clazz.cast(this);
          }
          return null;
        }

        public List<String> getQualifiedName() {
          return names;
        }

        public double getRowCount() {
          // use something other than 0 to give costing tests
          // some room, and make emps bigger than depts for
          // join asymmetry
          if (Iterables.getLast(names).equals("EMP")) {
            return 1000;
          } else {
            return 100;
          }
        }

        public RelDataType getRowType() {
          return rowType;
        }

        public RelOptSchema getRelOptSchema() {
          return CustomSchema.this;
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

        public Expression getExpression(Class clazz) {
          return null;
        }

        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
          final RelDataType extendedRowType = typeFactory.builder()
              .addAll(rowType.getFieldList())
              .addAll(extendedFields)
              .build();
          return new CustomColumnSet(names, extendedRowType, collationList);
        }
      }

}
