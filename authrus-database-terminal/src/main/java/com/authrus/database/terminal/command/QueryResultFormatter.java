package com.authrus.database.terminal.command;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintStream;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import com.authrus.database.terminal.console.ConsoleRow;
import com.authrus.database.terminal.console.ConsoleTable;
import com.authrus.database.terminal.console.ConsoleTableBuilder;

public class QueryResultFormatter implements CommandFormatter<QueryResult> {

   public String format(String command, QueryResult result) {
      List<List<String>> rows = result.getRows();
      int height = rows.size();
      
      if(height > 0) {
         List<String> titles = result.getColumns();
         ConsoleTable table = ConsoleTableBuilder.create(titles);
         String memory = result.getMemory();         
         long duration = result.getDuration();
         int width = titles.size();
         
         if(width > 0) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            PrintStream console = new PrintStream(buffer);
            
            for(int i = 0; i < height; i++) {
               List<String> cells = rows.get(i);
               ConsoleRow row = table.add();
               
               for(int j = 0; j < width; j++) {
                  String cell = cells.get(j);
                  row.set(j, cell);
               }         
            }

            console.println("> " + command);
            table.draw(console);    
            console.print("Finished (Time " + duration + " ms) (Required " + memory + ")");
            
            if (height > 0) {
               console.print(" (Result " + height + " rows)");
            }
            console.println();
            console.close();
            return buffer.toString(UTF_8);
         }
      }
      return null;
   }
}
