package com.authrus.database.attribute.transform;

import java.math.BigDecimal;

public class BigDecimalTransform implements ObjectTransform<BigDecimal, String> {

   public BigDecimal toObject(String value) {
      return new BigDecimal(value);
   }

   public String fromObject(BigDecimal value) {
      return value.toString();
   }
}
