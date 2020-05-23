package project.kachess.sql_lineage.persistable;

import project.kachess.shared.Util;
import project.kachess.sql_lineage.LineageDbService;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;

public class SelectItem {
  private Long id = null;
  private String name = null;
  private String definition = null;
  private SelectSectionType usageContext = null;
  private String alias = null;
  private String mapToSchema = null;
  private String mapToTable = null;
  private String mapToColumn = null;
  private Boolean isSimpleColumn = null;
  private String dataType = null;
  private String functionType = null;
  private Long datasetId = null;
  private Integer startLine = null;
  private Integer endLine = null;
  private Integer startPos = null;
  private Integer endPos = null;
  private String extraInfo = null;

  private HashSet<SelectItem> parentColumns = new HashSet<>();

  private LineageDbService dbService;

  public SelectItem(
      LineageDbService dbService,
      Long id,
      String itemDef,
      SelectSectionType usageCtx,
      String outputAlias,
      String map2Schema,
      String map2Table,
      String map2Column,
      Boolean isSimpleColumn,
      String dataType,
      FunctionType functionType,
      Long dsId,
      Integer sLnNum,
      Integer startPoint,
      Integer eLnNum,
      Integer endPoint) {
    this.id = id;
    this.definition = itemDef;
    this.usageContext = usageCtx;
    this.alias = outputAlias;
    this.mapToSchema = map2Schema;
    this.mapToTable = map2Table;
    this.mapToColumn = map2Column;
    this.isSimpleColumn = isSimpleColumn;
    this.dataType = dataType;
    this.functionType = functionType != null ? functionType.name() : null;
    this.datasetId = dsId;
    this.startLine = sLnNum;
    this.startPos = startPoint;
    this.endLine = eLnNum;
    this.endPos = endPoint;
    this.name = knownAs();
    this.dbService = dbService;
  }

  public SelectItem(LineageDbService dbService, Long newId, Long datasetId, SelectItem srcObj) {
    this.id = newId;
    this.definition = srcObj.definition;
    this.usageContext = srcObj.usageContext;
    this.alias = srcObj.alias;
    this.mapToSchema = srcObj.mapToSchema;
    this.mapToTable = srcObj.mapToTable;
    this.mapToColumn = srcObj.mapToColumn;
    this.isSimpleColumn = srcObj.isSimpleColumn;
    this.dataType = srcObj.dataType;
    this.functionType = srcObj.functionType;
    this.datasetId = datasetId;
    this.startLine = srcObj.startLine;
    this.startPos = srcObj.startPos;
    this.endLine = srcObj.endLine;
    this.endPos = srcObj.endPos;
    this.name = knownAs();
    this.dbService = dbService;
  }

  public String getName() {
    return name;
  }

  public String getDefinition() {
    return definition;
  }

  public String getAlias() {
    return alias;
  }

  public String getFunctionType() {
    return functionType;
  }

  public Long getDatasetId() {
    return datasetId;
  }

  public Integer getStartPos() {
    return startPos;
  }

  public Integer getEndPos() {
    return endPos;
  }

  public Integer getStartLine() {
    return startLine;
  }

  public Integer getEndLine() {
    return endLine;
  }

  public Long getId() {
    return id;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDatasetId(Long datasetId) {
    this.datasetId = datasetId;
  }

  public SelectSectionType getUsageContext() {
    return usageContext;
  }

  public String knownAs() {
    String defColName = null;
    if (isSimpleColumn != null && isSimpleColumn && definition != null) {
      // Returns c1 in a1.b1.c1
      String[] tabNameParts = Util.separateNamespaces(definition, 0);
      defColName = tabNameParts[tabNameParts.length - 1];
    }
    return Util.removeQuotes(
        Util.coalesce(alias, mapToColumn, defColName, String.format("_col_%d", id)));
  }

  public String getMapToSchema() {
    return mapToSchema;
  }

  public String getMapToTable() {
    return mapToTable;
  }

  public String getMapToColumn() {
    return mapToColumn;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public void setDefinition(String definition) {
    this.definition = definition;
  }

  public Boolean getSimpleColumn() {
    return isSimpleColumn;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getExtraInfo() {
    return extraInfo;
  }

  public void setExtraInfo(String extraInfo) {
    this.extraInfo = extraInfo;
  }

  public HashSet<SelectItem> getParentColumns() {
    return parentColumns;
  }

  public void setMapToSchema(String mapToSchema) {
    this.mapToSchema = mapToSchema;
  }

  public void setMapToTable(String mapToTable) {
    this.mapToTable = mapToTable;
  }

  public void setMapToColumn(String mapToColumn) {
    this.mapToColumn = mapToColumn;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void saveToDb() {
    /*
     * It would be nice to have some OR mapping capability here. For now, this works:
     */
    String prepSql =
        "INSERT IGNORE INTO select_item "
            + "(id, dataset_id, name, definition, usage_context, alias, map_to_schema, map_to_table, "
            + "map_to_column, is_simple_column, data_type, function_type, extra_info, "
            + "start_line, start_pos, end_line, end_pos, created_dt) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())"
            + " ON DUPLICATE KEY UPDATE "
            + "  dataset_id=?\n"
            + ", name=?\n"
            + ", definition=?\n"
            + ", usage_context=?\n"
            + ", alias=?\n"
            + ", map_to_schema=?\n"
            + ", map_to_table=?\n"
            + ", map_to_column=?\n"
            + ", is_simple_column=?\n"
            + ", data_type=?\n"
            + ", function_type=?\n"
            + ", extra_info=?\n"
            + ", start_line=?\n"
            + ", start_pos=?\n"
            + ", end_line=?\n"
            + ", end_pos=?\n"
            + ", created_dt=now()\n";
    try {
      PreparedStatement pstmt = dbService.getPreparedStatement(prepSql);
      int idx_cnt = 0;
      pstmt.setLong(++idx_cnt, getId());
      pstmt.setLong(++idx_cnt, getDatasetId());
      pstmt.setString(++idx_cnt, getName());
      pstmt.setString(++idx_cnt, getDefinition());
      pstmt.setString(++idx_cnt, getUsageContext().name());
      pstmt.setString(++idx_cnt, getAlias());
      pstmt.setString(++idx_cnt, getMapToSchema());
      pstmt.setString(++idx_cnt, getMapToTable());
      pstmt.setString(++idx_cnt, getMapToColumn());
      pstmt.setObject(++idx_cnt, getSimpleColumn());
      pstmt.setString(++idx_cnt, getDataType());
      pstmt.setString(++idx_cnt, getFunctionType());
      pstmt.setString(++idx_cnt, getExtraInfo());
      pstmt.setObject(++idx_cnt, getStartLine());
      pstmt.setObject(++idx_cnt, getStartPos());
      pstmt.setObject(++idx_cnt, getEndLine());
      pstmt.setObject(++idx_cnt, getEndPos());
      pstmt.setLong(++idx_cnt, getDatasetId());
      pstmt.setString(++idx_cnt, getName());
      pstmt.setString(++idx_cnt, getDefinition());
      pstmt.setString(++idx_cnt, getUsageContext().name());
      pstmt.setString(++idx_cnt, getAlias());
      pstmt.setString(++idx_cnt, getMapToSchema());
      pstmt.setString(++idx_cnt, getMapToTable());
      pstmt.setString(++idx_cnt, getMapToColumn());
      pstmt.setObject(++idx_cnt, getSimpleColumn());
      pstmt.setString(++idx_cnt, getDataType());
      pstmt.setString(++idx_cnt, getFunctionType());
      pstmt.setString(++idx_cnt, getExtraInfo());
      pstmt.setObject(++idx_cnt, getStartLine());
      pstmt.setObject(++idx_cnt, getStartPos());
      pstmt.setObject(++idx_cnt, getEndLine());
      pstmt.setObject(++idx_cnt, getEndPos());
      dbService.runPreparedUpdate(pstmt);
    } catch (SQLException anyExp) {
      // TODO - need to think holistically how to handle RT Exceptions ...
    }
  }
}
