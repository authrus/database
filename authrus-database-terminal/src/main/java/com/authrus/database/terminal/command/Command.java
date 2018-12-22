package com.authrus.database.terminal.command;

import com.authrus.database.terminal.session.SessionContext;

public interface Command {
   CommandResult execute(SessionContext session, String expression, boolean execute) throws Exception;
}
