package project.kachess.shared;

import project.kachess.sqlparser.g4generated.BingqlBaseVisitor;
import project.kachess.sqlparser.g4generated.BingqlLexer;
import project.kachess.sqlparser.g4generated.BingqlParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.IOException;

public class SyntaxCheck extends BingqlBaseVisitor<Void> {
  public static void main(String[] args) {
    int retStat = 0;
    try {
      String rawInFn = args[0];
      CharStream rawInput = CharStreams.fromFileName(rawInFn);
      BingqlLexer lexer = new BingqlLexer(rawInput);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      BingqlParser parser = new BingqlParser(tokens);
      parser.removeErrorListeners();
      parser.addErrorListener(SyntaxErrorListener.INSTANCE);
      ParseTree exprTree = parser.program();
      SyntaxCheck lineageBuilder = new SyntaxCheck();
      lineageBuilder.visit(exprTree);
    } catch (ParseCancellationException syntaxErr) {
      syntaxErr.printStackTrace();
      retStat = 1;
    } catch (RecognitionException e) {
      e.printStackTrace();
      retStat = 2;
    } catch (IOException ioe) {
      ioe.printStackTrace();
      retStat = 3;
    }
    System.exit(retStat);
  }
  //
  //  @Override
  //  public Void visitGroup_by_columns(BingqlParser.Group_by_columnsContext ctx) {
  //    System.out.println(String.format("Inside visitGroup_by_columns: getText() = %s",
  // ctx.getText()));
  //    return super.visitGroup_by_columns(ctx);
  //  }
  //
  //  @Override
  //  public Void visitGroup_set_items(BingqlParser.Group_set_itemsContext ctx) {
  //    System.out.println(String.format("Inside visitGroup_set_items: getText() = %s",
  // ctx.getText()));
  //    return super.visitGroup_set_items(ctx);
  //  }
}
