package com.authrus.database.bind.table.attribute;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.Column;
import com.authrus.database.Database;
import com.authrus.database.Schema;
import com.authrus.database.attribute.AttributeSerializer;
import com.authrus.database.attribute.ObjectBuilder;
import com.authrus.database.bind.TableBinder;
import com.authrus.database.bind.table.TableBuilder;
import com.authrus.database.bind.table.TableContext;
import com.authrus.database.bind.table.statement.BeginStatement;
import com.authrus.database.bind.table.statement.CommitStatement;
import com.authrus.database.bind.table.statement.CreateStatement;
import com.authrus.database.bind.table.statement.DeleteStatement;
import com.authrus.database.bind.table.statement.DropStatement;
import com.authrus.database.bind.table.statement.InsertStatement;
import com.authrus.database.bind.table.statement.RollbackStatement;
import com.authrus.database.bind.table.statement.SelectCountStatement;
import com.authrus.database.bind.table.statement.SelectStatement;
import com.authrus.database.bind.table.statement.TruncateStatement;
import com.authrus.database.bind.table.statement.UpdateOrInsertStatement;
import com.authrus.database.bind.table.statement.UpdateStatement;

public class AttributeTableBuilder implements TableBuilder {
   
   private final AttributeSerializer serializer;
   private final AttributeSchemaScanner scanner;
   private final ObjectBuilder builder;
   private final Database database;
   private final int version;

   public AttributeTableBuilder(Database database) {
      this(database, 0);
   }
   
   public AttributeTableBuilder(Database database, int version) {
      this.builder = new RowBuilder();
      this.scanner = new AttributeSchemaScanner(builder);
      this.serializer = new AttributeSerializer(builder);
      this.database = database;
      this.version = version;
   }

   @Override
   public TableBinder createTable(String name, Class type, String key) {
      return createTable(name, type, key, Collections.EMPTY_LIST);
   }
   
   @Override
   public TableBinder createTable(String name, Class type, String key, List<Column> columns) {
      List<String> list = Arrays.asList(key);
      
      if(key == null) {
         throw new IllegalStateException("Primary key must not be null for " + type);
      }
      return createTable(name, type, list);
   }

   @Override
   public TableBinder createTable(String name, Class type, List<String> keys) {
      return createTable(name, type, keys, Collections.EMPTY_LIST);
   }
   
   @Override
   public TableBinder createTable(String name, Class type, List<String> keys, List<Column> columns) {
      Schema schema = scanner.createSchema(type, keys, columns);
      
      if(keys.isEmpty()) {
         throw new IllegalStateException("Primary key must contain at least one column for " + type);
      }
      if(version > 0) {
         return new DatabaseTable(database, schema, type, name + "_v" + version);
      }
      return new DatabaseTable(database, schema, type, name);
   }
   
   private class DatabaseTable implements TableBinder {
      
      private final AttributeRecordMapper mapper;
      private final TableContext context;
      private final Database database;
      private final Schema schema;
      private final String name;
      
      public DatabaseTable(Database database, Schema schema, Class type, String name) {
         this.mapper = new AttributeRecordMapper(serializer, schema);
         this.context = new TableContext(schema, mapper, type, name);
         this.database = database;
         this.schema = schema;
         this.name = name;
      }       
      
      @Override
      public String name() {
         return name;
      }

      @Override
      public Schema schema() {        
         return schema;
      }  
      
      public AtomicLong lastUpdate() {
         return context.getTimeStamp();
      }

      @Override
      public DropStatement drop() {
         return new DropStatement(database, context);
      }
      
      @Override
      public DropStatement dropIfExists() {
         return new DropStatement(database, context, true);
      }
      
      @Override
      public CreateStatement create() {
         return new CreateStatement(database, context);
      }      

      @Override
      public CreateStatement createIfNotExists() {
         return new CreateStatement(database, context, true);
      }      
      
      @Override
      public BeginStatement begin() {
         return new BeginStatement(database, context);
      }
      
      @Override
      public CommitStatement commit() {
         return new CommitStatement(database, context);
      }   
      
      @Override
      public RollbackStatement rollback() {
         return new RollbackStatement(database, context);
      }  

      @Override
      public SelectStatement select() {
         return new SelectStatement(database, context);
      }
      
      @Override
      public SelectCountStatement selectCount() {
         return new SelectCountStatement(database, context);
      }

      @Override
      public InsertStatement insert() {
         return new InsertStatement(database, context);
      }
      
      @Override
      public InsertStatement insertOrIgnore() {
         return new InsertStatement(database, context, true);
      }      

      @Override
      public UpdateStatement update() {
         return new UpdateStatement(database, context);
      }

      @Override
      public UpdateOrInsertStatement updateOrInsert() {
         return new UpdateOrInsertStatement(database, context);
      }
      
      @Override
      public DeleteStatement delete() {
         return new DeleteStatement(database, context);
      }
      
      @Override
      public TruncateStatement truncate() {
         return new TruncateStatement(database, context);
      }
   }
}
