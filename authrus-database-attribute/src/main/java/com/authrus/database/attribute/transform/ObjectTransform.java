package com.authrus.database.attribute.transform;

public interface ObjectTransform<O, V> {
   O toObject(V value) throws Exception;
   V fromObject(O object) throws Exception;
}
