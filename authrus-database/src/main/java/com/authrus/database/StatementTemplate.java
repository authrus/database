package com.authrus.database;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class StatementTemplate implements Statement {
   
   protected final Map<String, Comparable> attributes;
   protected final AtomicBoolean closed;
   
   protected StatementTemplate() {
      this.attributes = new LinkedHashMap<String, Comparable>();
      this.closed = new AtomicBoolean();
   }

   @Override
   public Statement set(String name, String value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Integer value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Long value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Double value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Boolean value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }      
   
   @Override
   public Statement set(String name, Float value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Character value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Byte value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Short value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }

   @Override
   public Statement set(String name, Date value) throws Exception {
      Comparable previous = attributes.put(name, value);      
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      if(previous != null) {
         throw new IllegalStateException("Value of '" + previous + "' has already been set for '" + name + "'");
      }
      return this;
   }   

   @Override
   public Statement close() {
      closed.set(true);
      return this;
   }
}
