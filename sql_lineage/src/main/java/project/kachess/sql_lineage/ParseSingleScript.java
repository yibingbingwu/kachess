package project.kachess.sql_lineage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import project.kachess.shared.SyntaxErrorListener;
import project.kachess.sql_lineage.util.AutoIncrement;
import project.kachess.sqlparser.g4generated.HqlsqlLexer;
import project.kachess.sqlparser.g4generated.HqlsqlParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.IOException;

public class ParseSingleScript {
  @Parameter(
      names = {"--input", "-i"},
      description = "Single input file",
      required = true)
  private String testInputFn = null;

  @Parameter(
      names = {"--trunc-db", "-t"},
      description = "If specified, truncate the database before running test",
      required = false)
  private boolean truncDb = false;

  @Parameter(
      names = {"--seed-id", "-s"},
      description = "If specified, auto_increment start with this number",
      required = false)
  private int seedId = 110100;

  public static void main(String... argv) {
    ParseSingleScript main = new ParseSingleScript();
    JCommander.newBuilder().addObject(main).build().parse(argv);
    main.run();
  }

  public void run() {
    try {
      // DbConnectionBuilder urlBuilder = new DbConnectionBuilder("HMET_");
      // String connUrl =
      //    urlBuilder.getConnUrl(
      //        String.format(
      //            "jdbc:mysql://127.0.0.1:9876/hive?user=%s&password=%s",
      //            System.getenv("_host_usr"), System.getenv("ABDULLAH")));
      // ExternalHiveMetaStore extStore = new ExternalHiveMetaStore(connUrl);
      ExternalHiveMetaStore extStore =
          new ExternalHiveMetaStore(
              "jdbc:mysql://localhost/hive?user=dw_app&autoReconnect=true&useSSL=false");

      // OperationalInfoStore opsStore = new
      // OperationalInfoStore("jdbc:mysql://localhost/afdb?user=dw_app");
      LineageDbService dbService =
          new LineageDbService(
              "jdbc:mysql://localhost/bingql?user=dw_app&autoReconnect=true&useSSL=false",
              extStore);

      AutoIncrement.setCounter((long) seedId);

      // TODO - need to paramterize
      JobContext jobCtx =
          new JobContext(
              dbService,
              "airflow",
              "test_run",
              "parse.single_script",
              testInputFn,
              "HIVE",
              "93f1ab5cc07a0461819626f5f7ebd251b8408123");

      if (truncDb) {
        dbService.truncateDb();
      }

      CharStream rawInput = CharStreams.fromFileName(testInputFn);
      HqlsqlLexer lexer = new HqlsqlLexer(rawInput);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      HqlsqlParser parser = new HqlsqlParser(tokens);
      parser.removeErrorListeners();
      parser.addErrorListener(SyntaxErrorListener.INSTANCE);
      ParseTree exprTree = parser.program();

      // Start extracting metadata here:
      SqlMetaDataExtractor lineageBuilder = new SqlMetaDataExtractor();
      lineageBuilder.initSession(jobCtx, dbService);

      lineageBuilder.visit(exprTree);
      lineageBuilder.closeSession();

      extStore.cleanup();
      dbService.cleanup();
      // opsStore.cleanup();

      System.out.println("Done parsing " + testInputFn + "\n--------------------\n");
    } catch (RecognitionException e) {
      e.printStackTrace();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
