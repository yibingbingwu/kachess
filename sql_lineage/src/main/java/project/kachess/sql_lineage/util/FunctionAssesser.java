package project.kachess.sql_lineage.util;

import project.kachess.sql_lineage.persistable.FunctionType;

public class FunctionAssesser {
  private String funcName = null;
  private FunctionType type = null;
  private String returnType = null;
  private String complexFieldType = null;

  private FunctionAssesser() {}

  public FunctionAssesser(String funcTxt) {}

  public String getFuncName() {
    return funcName;
  }

  public FunctionType getType() {
    return type;
  }

  public String getReturnType() {
    return returnType;
  }

  public String getComplexFieldType(String structField) {
    return complexFieldType;
  }
}
