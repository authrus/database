package com.authrus.database.terminal.console;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class ConsoleTable {
   
   private final List<TableElement> elements;
   private final TableHeader header;
   private final String[] columns;
   private final int[] widths;
   
   public ConsoleTable(String... columns){
      this.elements = new ArrayList<TableElement>();
      this.header = new TableHeader(columns);
      this.widths = new int[columns.length];
      this.columns = columns;
   }
   
   public ConsoleRow add(Object... row){
      String[] values = new String[widths.length];
      TableRow element = new TableRow(values);
      
      for(int i = 0; i < row.length; i++){
         Object value = row[i];
         
         if(row[i] == null) {
            values[i] = "NULL"; // big fat null
         } else {
            values[i] = String.valueOf(value);
         }         
      }
      elements.add(element);
      
      return new ConsoleRow(columns, values);
   }

   public void draw(PrintStream console){
      draw(console, 60);
   }
   
   public void draw(PrintStream console, int limit){
      for(TableElement element : elements){
         element.stretch(widths);
      }
      header.stretch(widths);
      
      for(int i = 0; i < widths.length; i++){
         int width = widths[i];
         int space = width + 2;         
         
         widths[i] = Math.min(space, limit);
      }
      header.draw(console, widths);
      
      for(TableElement element : elements){
         element.draw(console, widths);
      }
      console.flush();
   }   
   
   private static abstract class TableElement{
      
      protected final String[] values;
      
      public TableElement(String[] values){
         this.values = values;
      }
      
      public void stretch(int[] widths){
         for(int i = 0; i < widths.length;i++){
            int length = values[i].length();
            
            if(length > widths[i]){
               widths[i] = length;
            }
         }
      } 
      
      public abstract void draw(PrintStream console, int[] widths);
   }
   
   private static class TableHeader extends TableElement {
      
      public TableHeader(String[] columns){
         super(columns);
      }
      
      @Override
      public void draw(PrintStream console, int[] widths){
         for(int i = 0; i < values.length; i++) {
            console.print("+");
            
            for(int j = 0; j < widths[i]; j++) {
               console.print('-');
            }
         }
         console.print("+\r\n|");
        
         for(int i = 0; i < values.length; i++) {
            String value = values[i];
            int size = value.length();
            int limit = widths[i];
            
            for(int j = 0; j < size && j < limit; j++) {
               char next = value.charAt(j);
               console.print(next);
            }
            for(int j = size; j < limit; j++) {
               console.print(" ");
            }            
            console.print("|");
         }
         console.print("\r\n");
         
         for(int i = 0; i < values.length; i++) {
            console.print("+");
            
            for(int j = 0; j < widths[i]; j++) {
               console.print('=');
            }
         }
         console.print("+\r\n"); 
      }
   }
   
   private static class TableRow extends TableElement {
      
      public TableRow(String[] values){
         super(values);
      } 
      
      @Override
      public void draw(PrintStream console, int[] widths){
         console.print("|");
         
         for(int i = 0; i < values.length; i++) {
            String value = values[i];
            int size = value.length();
            int limit = widths[i];
            
            for(int j = 0; j < size && j < limit; j++) {
               char next = value.charAt(j);
               console.print(next);
            }
            for(int j = size; j < limit; j++) {
               console.print(" ");
            }   
            console.print("|");
         }
         console.print("\r\n");
         
         for(int i = 0; i < values.length; i++) {
            console.print("+");
            
            for(int j = 0; j < widths[i]; j++) {
               console.print('-');
            }
         }
         console.print("+\r\n");  
      }
   }

}
