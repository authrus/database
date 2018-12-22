package com.authrus.database.sql.build;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;

public class IdentityConverter implements QueryConverter<Query> {

   @Override
   public Query convert(Query query) {
      return query;
   }

}
