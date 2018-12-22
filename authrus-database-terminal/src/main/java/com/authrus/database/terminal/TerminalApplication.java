package com.authrus.database.terminal;

import com.authrus.database.service.EnableResourceServer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@EnableResourceServer
@SpringBootApplication
public class TerminalApplication {

   public static void main(String[] list) throws Exception {
      SpringApplicationBuilder builder = new SpringApplicationBuilder(TerminalApplication.class);
      builder.web(WebApplicationType.NONE).run(list);
   }
}