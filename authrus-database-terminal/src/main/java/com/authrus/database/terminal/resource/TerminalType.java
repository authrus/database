package com.authrus.database.terminal.resource;

public enum TerminalType {
   TEXT,
   JSON;
   
   public boolean isText() {
      return this == TEXT;
   }
}
