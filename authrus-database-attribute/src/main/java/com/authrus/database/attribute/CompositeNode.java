package com.authrus.database.attribute;

import java.util.List;

import com.authrus.database.attribute.transform.ObjectTransformer;

public class CompositeNode implements ObjectNode {

   private final List<FieldAccessor> fields;
   private final ObjectTransformer transformer;
   private final SectionGenerator generator;
   private final ObjectBuilder builder;
   private final Class type;

   public CompositeNode(SectionGenerator generator, ObjectTransformer transformer, ObjectBuilder builder, List<FieldAccessor> fields, Class type) {
      this.transformer = transformer;
      this.generator = generator;
      this.builder = builder;
      this.fields = fields;
      this.type = type;
   }

   @Override
   public boolean isTransform() {
      return false;
   }

   @Override
   public boolean isPrimitive() {
      return false;
   }

   @Override
   public List<FieldAccessor> getFields() {
      return fields;
   }

   @Override
   public Converter getConverter(ObjectGraph graph) {
      return new CompositeConverter(generator, graph, transformer, this, type);
   }

   @Override
   public Object getInstance() {
      return getInstance(type);
   }

   @Override
   public Object getInstance(Class type) {
      return builder.createInstance(type);
   }

   @Override
   public Class getType() {
      return type;
   }
}
