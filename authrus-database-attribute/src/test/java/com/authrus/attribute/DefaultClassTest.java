package com.authrus.attribute;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.attribute.AttributeReader;
import com.authrus.database.attribute.AttributeSerializer;
import com.authrus.database.attribute.CombinationBuilder;
import com.authrus.database.attribute.MapReader;
import com.authrus.database.attribute.ObjectBuilder;
import com.authrus.database.attribute.ReflectionBuilder;
import com.authrus.database.attribute.SerializationBuilder;

import junit.framework.TestCase;

public class DefaultClassTest extends TestCase {
   
   public static class ExampleWithList implements Serializable {
      List<String> list;
      String name;
      public ExampleWithList(){
         this.list = new ArrayList<String>();
      }
   }
   
   public void testExample() throws Exception {
      Map<String, Object> values = new HashMap<String, Object>();
      values.put("class", "com.authrus.attribute.DefaultClassTest$ExampleWithList");
      values.put("name", "Some Name");
      values.put("list[0]",  "List entry 0");
      values.put("list[1]",  "List entry 1");
      values.put("list[2]",  "List entry 2");
      
      Set<ObjectBuilder> sequence = new LinkedHashSet<ObjectBuilder>();
      sequence.add(new ReflectionBuilder());
      sequence.add(new SerializationBuilder());
      ObjectBuilder factory = new CombinationBuilder(sequence);
      AttributeSerializer serializer = new AttributeSerializer(factory);
      AttributeReader reader = new MapReader(values);
      Object value = serializer.read(reader);      
      ExampleWithList example = (ExampleWithList)value;      
      
      assertNotNull(example);
      assertEquals(example.name, "Some Name");
      assertNotNull(example.list);      
      assertEquals(example.list.getClass(), ArrayList.class);
      assertEquals(example.list.size(), 3);
   }
}
