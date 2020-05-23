package project.kachess.sql_lineage.util;

/**
 * To indicate a Column is not found in the parent DatasetPool So the higher level caller may add
 * more context to the logging
 */
public class ColumnNotFound extends RuntimeException {
  public ColumnNotFound() {
    super();
  }

  public ColumnNotFound(String msg) {
    super(msg);
  }
}
