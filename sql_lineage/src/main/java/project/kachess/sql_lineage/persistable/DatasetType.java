package project.kachess.sql_lineage.persistable;

public enum DatasetType {
  SUBQUERY
  , CTE
  , TABLE
  , LATERAL_VIEW
  , TEMPORARY_TABLE
}
