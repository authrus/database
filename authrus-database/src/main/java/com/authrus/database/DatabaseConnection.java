package com.authrus.database;

public interface DatabaseConnection {
   Statement prepareStatement(String expression) throws Exception;
   Statement prepareStatement(String expression, boolean cache) throws Exception;
   void executeStatement(String expression) throws Exception;
   void closeConnection() throws Exception;
}
