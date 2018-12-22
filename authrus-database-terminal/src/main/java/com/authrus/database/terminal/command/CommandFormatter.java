package com.authrus.database.terminal.command;

public interface CommandFormatter<T> {
   String format(String expression, T value);
}
