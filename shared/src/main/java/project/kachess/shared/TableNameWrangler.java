package project.kachess.shared;

/** A Helper class that standardize how table references in SQL are handled */
public class TableNameWrangler {
  private String dbName;
  private String tabName;
  private String defSchema;

  public TableNameWrangler(String rawTxt, String defSchema) {
    this.defSchema = defSchema;
    String[] tabNameParts = Util.separateNamespaces(rawTxt.replaceAll("`", ""), 2);
    dbName = (tabNameParts[0] != null) ? Util.removeQuotes(tabNameParts[0]) : null;
    tabName = (tabNameParts[1] != null) ? Util.removeQuotes(tabNameParts[1]) : null;
  }

  public String getFullName() {
    return getDbName(true) + '.' + tabName;
  }

  public String getDbName(boolean notNull) {
    return (notNull) ? Util.coalesce(dbName, defSchema) : dbName;
  }

  public String getTabName() {
    return tabName;
  }
}
