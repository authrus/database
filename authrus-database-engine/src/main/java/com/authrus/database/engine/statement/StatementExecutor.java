package com.authrus.database.engine.statement;

import com.authrus.database.StatementTemplate;
import com.authrus.database.sql.compile.QueryCompiler;

public abstract class StatementExecutor extends StatementTemplate {
   
   protected final QueryCompiler compiler;
   
   protected StatementExecutor() {
      this.compiler = new QueryCompiler();
   }
}
