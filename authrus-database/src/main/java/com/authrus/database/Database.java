package com.authrus.database;

public interface Database {
   DatabaseConnection getConnection() throws Exception;
}
