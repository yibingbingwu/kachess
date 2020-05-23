package project.kachess.sql_lineage;

import project.kachess.sql_lineage.persistable.Dataset;

import java.util.TreeSet;

// This class helps CTE to be used in one than one places with a different alias
public class DatasetWrapper {
  String alias;
  Dataset dsObj;
  TreeSet<String> skipColNameList;
}
