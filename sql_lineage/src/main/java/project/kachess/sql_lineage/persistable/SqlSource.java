package project.kachess.sql_lineage.persistable;


import project.kachess.sql_lineage.LineageDbService;
import project.kachess.sql_lineage.util.AutoIncrement;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlSource {
  private Long id = null;
  private String sourceName = null;
  private String sourceLocator = null;
  private String sourceInfoExtra = null;
  private String sqlDialect = null;
  private String version = null;

  private LineageDbService dbService;

  private SqlSource() {
  }

  public SqlSource(LineageDbService dbService, String srcName, String srcId, String srcInfo, String sqlType, String version) {
    this.id = AutoIncrement.nextId();
    this.sourceName = srcName;
    this.sourceLocator = srcId;
    this.sourceInfoExtra = srcInfo;
    this.sqlDialect = sqlType;
    this.version = version;
    this.dbService = dbService;
  }

  public Long getId() {
    return id;
  }

  public String getSourceName() {
    return sourceName;
  }

  public String getSourceLocator() {
    return sourceLocator;
  }

  public String getSourceInfoExtra() {
    return sourceInfoExtra;
  }

  public String getSqlDialect() {
    return sqlDialect;
  }

  public String getVersion() {
    return version;
  }

  public void saveToDb() {
    String prepSql =
        "INSERT INTO sql_source "
            + "(id, source_name, source_locator, source_info_extra, sql_dialect, version, created_dt) "
            + "VALUES (?, ?, ?, ?, ?, ?, now())";
    try {
      PreparedStatement pstmt = dbService.getPreparedStatement(prepSql);
      pstmt.setLong(1, getId());
      pstmt.setString(2, getSourceName());
      pstmt.setString(3, getSourceLocator());
      pstmt.setString(4, getSourceInfoExtra());
      pstmt.setString(5, getSqlDialect());
      pstmt.setString(6, getVersion());
      dbService.runPreparedUpdate(pstmt);
    } catch (SQLException anyExp) {
    }
  }
}
