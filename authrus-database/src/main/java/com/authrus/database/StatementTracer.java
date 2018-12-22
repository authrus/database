package com.authrus.database;

import java.util.Date;

public class StatementTracer<T> implements Statement {
   
   private final Statement statement;
   private final Tracer<T> tracer;
   private final T value;
   
   public StatementTracer(Statement statement, Tracer<T> tracer, T value) {
      this.statement = statement;
      this.tracer = tracer;
      this.value = value;
   }

   @Override
   public ResultIterator<Record> execute() throws Exception {
      long start = System.currentTimeMillis();
      
      try {
         return statement.execute();
      } catch(Exception e) {
         throw new IllegalStateException("Error executing statement for " + value, e);
      } finally {
         long finish = System.currentTimeMillis();
         long duration = finish - start;
         
         tracer.trace(value, duration);
      }
   }

   @Override
   public Statement set(String name, String value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Integer value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Long value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Double value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Float value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Boolean value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Character value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Byte value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Short value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement set(String name, Date value) throws Exception {
      statement.set(name, value);
      return this;
   }

   @Override
   public Statement close() throws Exception {
      statement.close();
      return this;
   }

}
