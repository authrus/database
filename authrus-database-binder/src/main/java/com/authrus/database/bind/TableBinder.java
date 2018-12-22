package com.authrus.database.bind;

import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.Schema;
import com.authrus.database.bind.table.statement.BeginStatement;
import com.authrus.database.bind.table.statement.CommitStatement;
import com.authrus.database.bind.table.statement.CreateStatement;
import com.authrus.database.bind.table.statement.DeleteStatement;
import com.authrus.database.bind.table.statement.DropStatement;
import com.authrus.database.bind.table.statement.InsertStatement;
import com.authrus.database.bind.table.statement.RollbackStatement;
import com.authrus.database.bind.table.statement.SelectCountStatement;
import com.authrus.database.bind.table.statement.SelectStatement;
import com.authrus.database.bind.table.statement.TruncateStatement;
import com.authrus.database.bind.table.statement.UpdateOrInsertStatement;
import com.authrus.database.bind.table.statement.UpdateStatement;

public interface TableBinder<T> {
   DropStatement<T> drop();
   DropStatement<T> dropIfExists();
   BeginStatement<T> begin();
   CommitStatement<T> commit();
   RollbackStatement<T> rollback();
   CreateStatement<T> create();
   CreateStatement<T> createIfNotExists();
   SelectStatement<T> select();
   SelectCountStatement<T> selectCount();   
   UpdateOrInsertStatement<T> updateOrInsert();
   TruncateStatement<T> truncate();
   InsertStatement<T> insert();
   InsertStatement<T> insertOrIgnore();
   UpdateStatement<T> update();
   DeleteStatement<T> delete();
   AtomicLong lastUpdate();
   Schema schema();
   String name();
}
