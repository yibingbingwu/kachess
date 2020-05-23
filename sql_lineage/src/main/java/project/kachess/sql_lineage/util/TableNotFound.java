package project.kachess.sql_lineage.util;

public class TableNotFound extends RuntimeException {
  public TableNotFound(String msg) {
    super(msg);
  }
}
