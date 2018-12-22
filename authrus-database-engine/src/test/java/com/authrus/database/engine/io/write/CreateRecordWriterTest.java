package com.authrus.database.engine.io.write;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.common.io.InputStreamReader;
import com.authrus.database.common.io.OutputStreamWriter;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.OperationType;
import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.DataRecordWriter;
import com.authrus.database.engine.io.read.ChangeAssembler;
import com.authrus.database.engine.io.read.ChangeOperation;
import com.authrus.database.engine.io.read.ChangeSet;
import com.authrus.database.engine.io.read.CreateRecordReader;

public class CreateRecordWriterTest extends TestCase {
   
   public void testCreateRecord() throws Exception{
      ColumnSeries keys = new ColumnSeries();
      ColumnSeries columns = new ColumnSeries();
      PrimaryKey key = new PrimaryKey(keys);
      Properties properties = new Properties();
      Schema schema = new Schema(key, columns, properties);      
      
      keys.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "a", "a", 0));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "a", "a", 0));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.BYTE, null, "b", "b", 1));
      columns.addColumn(new Column(DataConstraint.UNIQUE, DataType.INT, null, "c", "c", 2));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.LONG, "12222", "d", "d", 3));
      
      CreateRecordWriter createWriter = new CreateRecordWriter("origin", schema);    
      ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
      OutputStreamWriter dataWriter = new OutputStreamWriter(outputBuffer);
      DataRecordWriter recordWriter = new DataRecordWriter(dataWriter);
      
      createWriter.write(recordWriter, null);
      
      byte[] result = outputBuffer.toByteArray();
      ByteArrayInputStream inputSource = new ByteArrayInputStream(result);
      InputStreamReader dataReader = new InputStreamReader(inputSource);
      DataRecordReader recordReader = new DataRecordReader(dataReader);
      
      char code = recordReader.readChar();
      OperationType type = OperationType.resolveType(code);
      String origin = recordReader.readString();     
      
      assertEquals(origin, "origin");
      assertEquals(type, OperationType.CREATE);   
      
      CreateRecordReader createReader = new CreateRecordReader(origin, "table");              
      ChangeOperation operation = createReader.read(recordReader);
      final AtomicReference<Schema> update = new AtomicReference<Schema>();
      
      operation.execute(new ChangeAssembler() {
         @Override
         public void onCreate(String origin, String name, Schema schema) {
            update.set(schema);
         }
         public void onBegin(String origin, String name, Transaction transaction) {}
         public void onInsert(String origin, String name, ChangeSet change) {}
         public void onUpdate(String origin, String name, ChangeSet change) {}
         public void onDelete(String origin, String name, String key) {}
         public void onIndex(String origin, String name, String column) {}
         public void onCommit(String origin, String name) {}
         public void onDrop(String origin, String name) {}         
         public void onRollback(String user, String name) {}
         
      });
      
      
      assertNotNull(update.get());
      assertEquals(update.get().getCount(), 4);
      assertEquals(update.get().getColumn(0).getName(), "a");
      assertEquals(update.get().getColumn(1).getName(), "b");
      assertEquals(update.get().getColumn(2).getName(), "c");
      assertEquals(update.get().getColumn(3).getName(), "d");
      assertEquals(update.get().getColumn(0).getDataConstraint(), DataConstraint.REQUIRED);
      assertEquals(update.get().getColumn(1).getDataConstraint(), DataConstraint.OPTIONAL);
      assertEquals(update.get().getColumn(2).getDataConstraint(), DataConstraint.UNIQUE);
      assertEquals(update.get().getColumn(3).getDataConstraint(), DataConstraint.REQUIRED);
      assertEquals(update.get().getColumn(0).getDataType(), DataType.TEXT);
      assertEquals(update.get().getColumn(1).getDataType(), DataType.BYTE);
      assertEquals(update.get().getColumn(2).getDataType(), DataType.INT);
      assertEquals(update.get().getColumn(3).getDataType(), DataType.LONG);
   }

}
