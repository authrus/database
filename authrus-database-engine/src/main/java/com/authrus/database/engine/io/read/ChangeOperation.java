package com.authrus.database.engine.io.read;

public interface ChangeOperation {
   boolean execute(ChangeAssembler assembler);
}
