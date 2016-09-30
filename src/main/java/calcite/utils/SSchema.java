package calcite.utils;

import java.util.List;

import com.google.common.collect.Lists;

public class SSchema {
	protected static final String DEFAULT_CATALOG = "CATALOG";
    final List<String> tableNames = Lists.newArrayList();
    String name;

    public SSchema(String name) {
      this.name = name;
    }

    public void addTable(String name) {
      tableNames.add(name);
    }

    public String getCatalogName() {
      return DEFAULT_CATALOG;
    }

    public String getName() {
      return name;
    }
}