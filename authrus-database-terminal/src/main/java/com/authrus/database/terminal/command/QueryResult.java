package com.authrus.database.terminal.command;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResult {

   private List<List<String>> rows;
   private List<String> columns;
   private String memory;
   private long duration;
}
