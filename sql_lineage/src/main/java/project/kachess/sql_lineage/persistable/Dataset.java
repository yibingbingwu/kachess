package project.kachess.sql_lineage.persistable;

import project.kachess.sql_lineage.LineageDbService;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class Dataset {
  /*
      Use Java object instead of primitives b/c it is easier
      to generate SQL insert using Java Reflection
      But this does mess up Getters, esp Boolean/boolean getters
  */
  private Long id = null;
  private DatasetType type = null;
  private String mapToSchema = null;
  private String mapToTable = null;
  private String definedName = null; // e.g. CTE name, or who knows what else
  private Boolean isAggregated = null;
  private Integer startLine = null;
  private Integer endLine = null;
  private Integer startPos = null;
  private Integer endPos = null;

  private String storageLoc = null;
  private List<SelectItem> columnList = new ArrayList<>();
  private List<SelectItem> filterColList = new ArrayList<>();
  private List<SelectItem> joinColList = new ArrayList<>();
  private boolean isUnionBase = false;

  // If the DS is copy of a physical table, should not write back to metadata db
  private boolean isReadOnly = false;

  private LineageDbService dbService;

  private Dataset() {
  }

  public Dataset(
      LineageDbService dbService,
      Long id,
      DatasetType type,
      String dbSchema,
      String dbTable,
      Integer sLnNum,
      Integer startPos,
      Integer eLnNum,
      Integer endPos,
      Boolean isAggregated) {
    this.id = id;
    this.type = type;
    this.mapToSchema = dbSchema;
    this.mapToTable = dbTable;
    this.startLine = sLnNum;
    this.endLine = eLnNum;
    this.startPos = startPos;
    this.endPos = endPos;
    this.isAggregated = isAggregated;
    this.dbService = dbService;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Integer getStartLine() {
    return startLine;
  }

  public Integer getEndLine() {
    return endLine;
  }

  public Integer getStartPos() {
    return startPos;
  }

  public Boolean getAggregated() {
    return isAggregated;
  }

  public DatasetType getType() {
    return type;
  }

  public String getMapToSchema() {
    return mapToSchema;
  }

  public String getMapToTable() {
    return mapToTable;
  }

  public Integer getEndPos() {
    return endPos;
  }

  public void setEndLine(Integer endLine) {
    this.endLine = endLine;
  }

  public void setEndPos(Integer endPos) {
    this.endPos = endPos;
  }

  public List<SelectItem> columnList() {
    return columnList;
  }

  public void setColumnList(List<SelectItem> columnList) {
    assertNotNull(columnList);
    this.columnList = columnList;
  }

  public List<SelectItem> getFilterColList() {
    return filterColList;
  }

  public void setFilterColList(List<SelectItem> filterColList) {
    this.filterColList = filterColList;
  }

  public void setJoinColList(List<SelectItem> joinColList) {
    this.joinColList = joinColList;
  }

  public void setType(DatasetType type) {
    this.type = type;
  }

  public String getStorageLoc() {
    return storageLoc;
  }

  public void setStorageLoc(String storageLoc) {
    this.storageLoc = storageLoc;
  }

  /**
   * Indicate whether this is the first Dataset to be unioned (with the second)
   */
  public void setIsUnionBase(boolean yn) {
    isUnionBase = yn;
  }

  public boolean canBeUnioned() {
    return isUnionBase;
  }

  public boolean isReadOnly() {
    return isReadOnly;
  }

  public void setReadOnly(boolean readOnly) {
    isReadOnly = readOnly;
  }

  public String getDefinedName() {
    return definedName;
  }

  public void setDefinedName(String definedName) {
    this.definedName = definedName;
  }

  // TODO - should really make an Interface but settle for adhoc for now
  public void saveToDb() {
    if (isReadOnly()) {
      return;
    }

    /*
     * It would be nice to have some OR mapping capability here. For now, this works:
     */
    String prepSql =
        "REPLACE INTO dataset "
            + "(id, type, map_to_schema, map_to_table, is_aggregated, start_line, start_pos, end_line, end_pos, " +
            "created_dt) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())";
    try {
      PreparedStatement pstmt = dbService.getPreparedStatement(prepSql);
      int idx_cnt = 0;
      pstmt.setLong(++idx_cnt, getId());
      pstmt.setString(++idx_cnt, getType().name());
      pstmt.setString(++idx_cnt, getMapToSchema());
      pstmt.setString(++idx_cnt, getMapToTable());
      pstmt.setObject(++idx_cnt, getAggregated());
      pstmt.setObject(++idx_cnt, getStartLine());
      pstmt.setObject(++idx_cnt, getStartPos());
      pstmt.setObject(++idx_cnt, getEndLine());
      pstmt.setObject(++idx_cnt, getEndPos());
      dbService.runPreparedUpdate(pstmt);
    } catch (SQLException anyExp) {
      // TODO - need to think holistically how to handle RT Exceptions ...
    }

    for (SelectItem currCol : columnList) {
      currCol.saveToDb();

      // Start adding parent/child relationship, using the parent info carried with each item, for
      // the items after SELECT
      for (SelectItem currPsc : currCol.getParentColumns()) {
        dbService.addSelectColumnPair(currCol, currPsc, SelectSectionType.SELECT);
      }

      // Now adding cross sectional dependencies, such as WHERE:
      for (SelectItem currFsc : filterColList) {
        dbService.addSelectColumnPair(currCol, currFsc, SelectSectionType.WHERE);
      }

      // Now JOIN:
      for (SelectItem currJsc : joinColList) {
        dbService.addSelectColumnPair(currCol, currJsc, SelectSectionType.JOIN);
      }
    }
  }

}
