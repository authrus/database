package com.authrus.database.terminal.resource;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path(value = TerminalResource.RESOURCE_PATH)
@Api(value = TerminalResource.RESOURCE_NAME, produces = MediaType.APPLICATION_JSON)
public class TerminalResource {
   
   public static final String RESOURCE_PATH = "/v1/console";
   public static final String RESOURCE_NAME = "console";
   
   private final TerminalService terminal;
   
   @Inject
   public TerminalResource(@Context TerminalService terminal) {
      this.terminal = terminal;
   }
   
   @POST  
   @Produces(MediaType.APPLICATION_JSON)
   @ApiOperation(value = "Execute command")
   public void execute(
         @Suspended AsyncResponse response,
         TerminalRequest request) 
   {
      terminal.submit(request)
         .exceptionally((cause) -> {
            String message = cause.getMessage();
            return TerminalResult.builder()
                  .error(message)
                  .build();
         })      
         .thenAccept((result) -> {
            Response entity = Response.ok(result).build();
            response.resume(entity);
         });
   }
   
}
