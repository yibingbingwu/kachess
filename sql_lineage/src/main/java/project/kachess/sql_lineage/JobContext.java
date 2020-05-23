package project.kachess.sql_lineage;

import project.kachess.sql_lineage.persistable.SqlSource;

import static org.junit.Assert.assertNotNull;

public class JobContext {
  // Assuming catalog can be null for now
  private String currentSchema = null;
  private SqlSource sqlSource = null;

  private JobContext() {
  }

  public JobContext(
      LineageDbService dbService,
      String srcName,
      String schema,
      String srcLocKey,
      String srcFn,
      String sqlType,
      String version) {
    assertNotNull(srcLocKey);
    currentSchema = schema;
    sqlSource = new SqlSource(dbService, srcName, srcLocKey, srcFn, sqlType, version);
    sqlSource.saveToDb();
  }

  public void setCurrentSchema(String currentSchema) {
    this.currentSchema = currentSchema;
  }

  public String getCurrentSchema() {
    assertNotNull(currentSchema);
    return currentSchema;
  }

  public SqlSource getSqlSource() {
    return sqlSource;
  }

  public String getSystemSource() {
    return sqlSource.getSourceName();
  }
}
