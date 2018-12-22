package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.BEGIN;
import static com.authrus.database.engine.OperationType.COMMIT;
import static com.authrus.database.engine.OperationType.CREATE;
import static com.authrus.database.engine.OperationType.DELETE;
import static com.authrus.database.engine.OperationType.DROP;
import static com.authrus.database.engine.OperationType.INDEX;
import static com.authrus.database.engine.OperationType.INSERT;
import static com.authrus.database.engine.OperationType.ROLLBACK;
import static com.authrus.database.engine.OperationType.UPDATE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.authrus.database.Schema;
import com.authrus.database.engine.ChangeListener;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.Transaction;

public class ChangeLogPersister implements ChangeListener {
   
   private static final Logger LOG = LoggerFactory.getLogger(ChangeLogPersister.class);
   
   private final ChangeLog log;
   
   public ChangeLogPersister(ChangeLog log) {
      this.log = log;
   }   

   @Override
   public void onBegin(String origin, String table, Transaction transaction) {
      if(origin != null) {
         ChangeRecordWriter writer = new BeginRecordWriter(origin, transaction);
         ChangeRecord record = new ChangeRecord(writer, BEGIN, origin, table);
         
         try {
            log.log(record);            
         } catch(Exception e) {
            LOG.info("Unable to log begin event for '" + table + "'", e);
         }
      }
   }   

   @Override
   public void onCreate(String origin, String table, Schema schema) {
      if(origin != null) {
         ChangeRecordWriter writer = new CreateRecordWriter(origin, schema);
         ChangeRecord record = new ChangeRecord(writer, CREATE, origin, table);
         
         try {
            log.log(record);            
         } catch(Exception e) {
            LOG.info("Unable to log create event for '" + table + "'", e);
         }
      }
   }   
   
   @Override
   public void onInsert(String origin, String table, Row tuple) {
      if(origin != null) {
         ChangeRecordWriter writer = new InsertRecordWriter(origin, tuple);
         ChangeRecord record = new ChangeRecord(writer, INSERT, origin, table);
         
         try {
            log.log(record);            
         } catch(Exception e) {
            LOG.info("Unable to log insert event for '" + table + "'", e);
         }      
      }
   }    
   
   @Override
   public void onUpdate(String origin, String table, Row current, Row previous) {
      if(origin != null) {
         ChangeRecordWriter writer = new UpdateRecordWriter(origin, current, previous);
         ChangeRecord record = new ChangeRecord(writer, UPDATE, origin, table);
         
         try {
            log.log(record);         
         } catch(Exception e) {
            LOG.info("Unable to log update event for '" + table + "'", e);
         }      
      }
   }   
   
   @Override
   public void onDelete(String origin, String table, String key) {
      if(origin != null) {
         ChangeRecordWriter writer = new DeleteRecordWriter(origin, key);
         ChangeRecord record = new ChangeRecord(writer, DELETE, origin, table);
         
         try {         
            log.log(record);         
         } catch(Exception e) {
            LOG.info("Unable to log delete event for '" + table + "'", e);
         }      
      }
   }     

   @Override
   public void onIndex(String origin, String table, String column) {
      if(origin != null) {
         ChangeRecordWriter writer = new IndexRecordWriter(origin, column);
         ChangeRecord record = new ChangeRecord(writer, INDEX, origin, table);
         
         try {
            log.log(record);            
         } catch(Exception e) {
            LOG.info("Unable to log index event for '" + table + "'", e);
         }
      }
   }

   @Override
   public void onDrop(String origin, String table) {
      if(origin != null) {
         ChangeRecordWriter writer = new DropRecordWriter(origin);
         ChangeRecord record = new ChangeRecord(writer, DROP, origin, table);
         
         try {
            log.log(record);            
         } catch(Exception e) {
            LOG.info("Unable to log drop event for '" + table + "'", e);
         }
      }
   }

   @Override
   public void onCommit(String origin, String table) {
      if(origin != null) {
         ChangeRecordWriter writer = new CommitRecordWriter(origin);
         ChangeRecord record = new ChangeRecord(writer, COMMIT, origin, table);
         
         try {
            log.log(record);            
         } catch(Exception e) {
            LOG.info("Unable to log commit event for '" + table + "'", e);
         }
      }
   }

   @Override
   public void onRollback(String origin, String table) {
      if(origin != null) {
         ChangeRecordWriter writer = new RollbackRecordWriter(origin);
         ChangeRecord record = new ChangeRecord(writer, ROLLBACK, origin, table);
         
         try {
            log.log(record);            
         } catch(Exception e) {
            LOG.info("Unable to log rollback event for '" + table + "'", e);
         }
      }
   }
}
