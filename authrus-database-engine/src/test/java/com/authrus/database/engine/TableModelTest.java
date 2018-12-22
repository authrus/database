package com.authrus.database.engine;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.attribute.AttributeSerializer;
import com.authrus.database.attribute.ObjectBuilder;
import com.authrus.database.bind.table.attribute.RowBuilder;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.filter.ComparisonNode;
import com.authrus.database.engine.filter.FilterNode;
import com.authrus.database.engine.filter.RandomOrderFilter;

public class TableModelTest extends TestCase {
   
   
   public void testTableModel() throws Exception {
      ColumnSeries keys = new ColumnSeries();
      ColumnSeries columns = new ColumnSeries();
      PrimaryKey key = new PrimaryKey(keys);
      Properties properties = new Properties();
      Schema schema = new Schema(key, columns, properties);      
      
      keys.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "key", "key", 0));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "key", "key", 0));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name", 1));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "mail", "mail", 2));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "password", "password", 3));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "type", "type", 4));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "rank", "rank", 5));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "percentile", "percentile", 6));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.DOUBLE, null, "rating", "rating", 7));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.DOUBLE, null, "deviation", "deviation", 8));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.DOUBLE, null, "volatility", "volatility", 9));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "blackGames", "blackGames", 10));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "whiteGames", "whiteGames", 11));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.LONG, null, "lastSeenOnline", "lastSeenOnline", 12));
      
      Random random = new SecureRandom();
      List<String> domains = new ArrayList<String>();
      
      domains.add("yahoo.com");
      domains.add("gmail.com");
      domains.add("hotmail.com");
      domains.add("mail.com");
      
      Collections.shuffle(domains);      
      
      ObjectBuilder builder = new RowBuilder();
      AttributeSerializer serializer = new AttributeSerializer(builder);
      RowMapper converter = new RowMapper(serializer, schema); 
      ChangeListener listener = new AccountChangeListener();
      Catalog catalog = new Catalog(listener, "owner");      
      
      catalog.createTable("owner", "players", schema);
      
      Table table = catalog.findTable("players");
      
      assertNotNull(table);
      
      TableModel model = table.getModel();
      
      model.index(schema.getColumn("mail"));
      model.index(schema.getColumn("rating"));      
      catalog.beginTransaction("owner", "players", "1");
      
      for(int i = 0; i < 10; i++) {
         double rating = random.nextInt(100);
         int size = domains.size();
         int index = random.nextInt(size);
         String mail = random.nextInt(100) + "@" + domains.get(index);
         Account account = new Account(AccountType.PREMIUM, "key-" + i, "name-" + i, mail, "password-" + i);
         account.setRating(rating);
         Row tuple = converter.createTuple(account);
         
         model.insert(tuple);
      } 
      catalog.commitTransaction("owner", "players");
      
      catalog.beginTransaction("owner", "players", "2");
      
      for(int i = 10; i < 1000; i++) {
         double rating = random.nextInt(100);
         int size = domains.size();
         int index = random.nextInt(size);
         String mail = random.nextInt(100) + "@" + domains.get(index);
         Account account = new Account(AccountType.PREMIUM, "key-" + i, "name-" + i, mail, "password-" + i);
         account.setRating(rating);
         Row tuple = converter.createTuple(account);
         
         model.insert(tuple);
      }           
      Column column1 = schema.getColumn("rating");
      FilterNode node1 = new ComparisonNode(column1, 10.0, ">");
      RandomOrderFilter filter1 = new RandomOrderFilter(node1, 10000);         
      List<Row> tuples1 = model.list(filter1);
      assertTrue(!tuples1.isEmpty());
      
      for(Row tuple : tuples1) {
         System.err.println(tuple.getCell(column1.getIndex()).getValue());
      }

   
      Column column2 = schema.getColumn("mail");
      FilterNode node2 = new ComparisonNode(column2, "%yahoo.com", "like");
      RandomOrderFilter filter2 = new RandomOrderFilter(node2, 10000);      
      List<Row> tuples2 = model.list(filter2);
      
      for(Row tuple : tuples2) {
         Comparable mail = tuple.getCell(column2.getIndex()).getValue();
         
         System.err.println(mail);
         assertTrue(mail.toString().endsWith("@yahoo.com"));
      }
      assertEquals(model.size(), 1000);
      
      catalog.rollbackTransaction("owner", "players");
      
      // Show the ratings after the rollback!!
      List<Row> tuples3 = model.list(filter1);
      assertTrue(!tuples3.isEmpty());
      
      for(Row tuple : tuples3) {
         System.err.println(tuple.getCell(column1.getIndex()).getValue());
      }
   }
   
   
   public static enum AccountType {
      ANONYMOUS, PREMIUM, ACTIVE, INACTIVE;
   }

   public static class Account implements Serializable {

      private static final long serialVersionUID = 1L;

      private AccountType type; // 16 bytes
      private String key; // 20 bytes
      private String name; // 20 bytes
      private String mail; // 20 bytes
      private String password; // 30 bytes
      private int rank; // 4 bytes
      private int percentile; // 4 bytes
      private double rating; // 8 bytes
      private double deviation; // 8 bytes
      private double volatility; // 8 bytes
      private int blackGames; // 4 bytes
      private int whiteGames; // 4 bytes
      private long lastSeenOnline; // 8 bytes // 170 bytes!!!

      public Account(AccountType type, String key, String name, String mail, String password) {
         this.key = key.toLowerCase();
         this.password = password;
         this.type = type;
         this.name = name;
         this.mail = mail;
      }

      public AccountType getType() {
         return type;
      }

      public void setType(AccountType type) {
         this.type = type;
      }

      public String getMail() {
         return mail;
      }

      public void setMail(String mail) {
         this.mail = mail;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getPassword() {
         return password;
      }

      public void setPassword(String password) {
         this.password = password;
      }

      public String getKey() {
         return key;
      }

      public void setKey(String userId) {
         this.key = userId;
      }

      public int getRank() {
         return rank;
      }

      public void setRank(int rank) {
         this.rank = rank;
      }

      public int getPercentile() {
         return percentile;
      }

      public void setPercentile(int percentile) {
         this.percentile = percentile;
      }

      public double getRating() {
         return rating;
      }

      public void setRating(double rating) {
         this.rating = rating;
      }

      public double getVolatility() {
         return volatility;
      }

      public void setVolatility(double volatility) {
         this.volatility = volatility;
      }

      public double getDeviation() {
         return deviation;
      }

      public void setDeviation(double deviation) {
         this.deviation = deviation;
      }

      public int getBlackGames() {
         return blackGames;
      }

      public void setBlackGames(int blackGames) {
         this.blackGames = blackGames;
      }

      public int getWhiteGames() {
         return whiteGames;
      }

      public void setWhiteGames(int whiteGames) {
         this.whiteGames = whiteGames;
      }

      public long getLastSeenOnline() {
         return lastSeenOnline;
      }

      public void setLastSeenOnline(long lastSeenOnline) {
         this.lastSeenOnline = lastSeenOnline;
      }

      @Override
      public String toString() {
         return String.format("name=%s id=%s", name, key);
      }
   }
   
   private static class AccountChangeListener implements ChangeListener {

      @Override
      public void onBegin(String origin, String table, Transaction transaction) {
         System.err.println("onBegin("+origin+", "+table+")");
      }

      @Override
      public void onCreate(String origin, String table, Schema schema) {
         System.err.println("onCreate("+origin+", "+table+")");
      }

      @Override
      public void onInsert(String origin, String table, Row tuple) {
         System.err.println("onInsert("+origin+", "+table+", "+tuple+")");
      }

      @Override
      public void onUpdate(String origin, String table, Row current, Row previous) {
         System.err.println("onUpdate("+origin+", "+table+", "+current+", "+previous+")");
      }

      @Override
      public void onDelete(String origin, String table, String key) {
         System.err.println("onDelete("+origin+", "+table+")");
      }

      @Override
      public void onIndex(String origin, String table, String column) {
         System.err.println("onIndex("+origin+", "+table+")");
      }

      @Override
      public void onCommit(String origin, String table) {
         System.err.println("onCommit("+origin+", "+table+")");
      }

      @Override
      public void onRollback(String origin, String table) {
         System.err.println("onRollback("+origin+", "+table+")");
      }

      @Override
      public void onDrop(String origin, String table) {
         System.err.println("onDrop("+origin+", "+table+")");
      }      
   }

}
