package com.authrus.database.sql;

import java.util.List;

import com.authrus.database.sql.parse.QueryError;
import com.authrus.database.sql.parse.QueryLexicalAnalyzer;
import com.authrus.database.sql.parse.QueryToken;

import junit.framework.TestCase;

public class QueryLexicalAnalyzerTest extends TestCase {

   public void testUpdate() throws Exception {
      QueryLexicalAnalyzer analyzer = new QueryLexicalAnalyzer("update x set a=b,c=d,e=f");
      List<QueryToken> tokens = analyzer.getTokens();
      
      for(QueryToken token : tokens) {
         System.err.println(token.getType()+"("+token+")");
      }
      List<QueryError> errors = analyzer.getErrors();
      
      for(QueryError error : errors) {
         System.err.println(error.getType() +":------>>"+error+"<<--------");
      }
      assertTrue(analyzer.isSuccess());
   }
   
   public void testOrderBy() throws Exception {
      QueryLexicalAnalyzer analyzer = new QueryLexicalAnalyzer("select x from y order by z asc");
      List<QueryToken> tokens = analyzer.getTokens();
      
      for(QueryToken token : tokens) {
         System.err.println(token.getType()+"("+token+")");
      }
      List<QueryError> errors = analyzer.getErrors();
      
      for(QueryError error : errors) {
         System.err.println(error.getType() +":------>>"+error+"<<--------");
      }
      assertTrue(analyzer.isSuccess());
   }
   
   public void testInsert() throws Exception {
      QueryLexicalAnalyzer analyzer = new QueryLexicalAnalyzer("insert into table (x,y, z)values (?,?,?) ");
      List<QueryToken> tokens = analyzer.getTokens();
      
      for(QueryToken token : tokens) {
         System.err.println(token.getType()+"("+token+")");
      }
      List<QueryError> errors = analyzer.getErrors();
      
      for(QueryError error : errors) {
         System.err.println(error.getType() +":------>>"+error+"<<--------");
      }
      assertTrue(analyzer.isSuccess());
   }
   
   
   public void testNamedParameters() throws Exception {
      QueryLexicalAnalyzer analyzer = new QueryLexicalAnalyzer("select name, blah , top,x  from table where x = :x and y = :blah or x = :foo ");
      List<QueryToken> tokens = analyzer.getTokens();
      
      for(QueryToken token : tokens) {
         System.err.println(token.getType()+"("+token+")");
      }
      List<QueryError> errors = analyzer.getErrors();
      
      for(QueryError error : errors) {
         System.err.println(error.getType() +":------>>"+error+"<<--------");
      }
      assertTrue(analyzer.isSuccess());
   }

   public void testDifferenceCase() throws Exception {
      QueryLexicalAnalyzer analyzer = new QueryLexicalAnalyzer(
            "SELECT * FROM table\r\n"+
            "  WHERE x = ?\r\n"+
            "     AND y = ?\r\n"+
            "     OR  x = ?");
      List<QueryToken> tokens = analyzer.getTokens();
      
      for(QueryToken token : tokens) {
         System.err.println(token.getType()+"("+token+")");
      }
      assertTrue(analyzer.isSuccess());
   }
   
   public void testSimpleWithWhereConditions() throws Exception {
      QueryLexicalAnalyzer analyzer = new QueryLexicalAnalyzer("select * from table where x = ? and y = ? or x = ?");
      List<QueryToken> tokens = analyzer.getTokens();
      
      for(QueryToken token : tokens) {
         System.err.println(token.getType()+"("+token+")");
      }
      assertTrue(analyzer.isSuccess());
   }
   
   public void testSimpleSelect() throws Exception {
      QueryLexicalAnalyzer analyzer = new QueryLexicalAnalyzer("select * from table where x = ?");
      List<QueryToken> tokens = analyzer.getTokens();
      
      for(QueryToken token : tokens) {
         System.err.println(token.getType()+"("+token+")");
      }
      assertTrue(analyzer.isSuccess());
   }
   

}
