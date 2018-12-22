package com.authrus.database.terminal.command;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.output.ByteArrayOutputStream;
import com.authrus.database.terminal.console.ConsoleTable;
import com.authrus.database.terminal.console.ConsoleTableBuilder;

import com.google.common.collect.Lists;

public class CollectionFormatter implements CommandFormatter<Collection<?>> {

   @Override
   public String format(String command, Collection<?> collection) {
      List<?> list = Lists.newArrayList(collection);
      Class<?> type = list.stream()
            .filter(Objects::nonNull)
            .map(Object::getClass)
            .findFirst()
            .orElse(null);
      
      if(type != null) {
         Field[] fields = type.getDeclaredFields();
         String[] names = Arrays.asList(fields)
               .stream()
               .map(Field::getName)
               .toArray(String[]::new);
         
         ConsoleTable table = ConsoleTableBuilder.create(names);
         int height = list.size();
         
         if(height > 0) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            PrintStream console = new PrintStream(buffer);
            
            for(int i = 0; i < height; i++) {
               Object value = list.get(i);
               Object[] cells = Arrays.asList(fields)
                     .stream()
                     .map(field -> {
                        try {
                           field.setAccessible(true);
                           return field.get(value);
                        } catch(Exception e) {
                           return null;
                        }
                     })
                     .toArray(Object[]::new);
               
               table.add(cells);
            }            
            table.draw(console);
            console.close();
            return buffer.toString(UTF_8);
         }
      }
      return null;
   }

}
