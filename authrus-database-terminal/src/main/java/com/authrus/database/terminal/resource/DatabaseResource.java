package com.authrus.database.terminal.resource;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.authrus.database.terminal.command.QueryResult;

@Path(value = DatabaseResource.RESOURCE_PATH)
@Api(value = DatabaseResource.RESOURCE_NAME, produces = MediaType.APPLICATION_JSON)
public class DatabaseResource {
   
   public static final String RESOURCE_PATH = "/v1/database";
   public static final String RESOURCE_NAME = "database";
   
   private final DatabaseService service;
   
   @Inject
   public DatabaseResource(@Context DatabaseService service) {
      this.service = service;
   }
   
   @POST  
   @Path("/create")
   @ApiOperation(value = "Create table command")
   public Response create(DatabaseRequest request) { 
      service.create(request);
      return Response.noContent().build();
   }
   
   @POST  
   @Path("/insert")
   @ApiOperation(value = "Insert row command")
   public Response insert(DatabaseRequest request) { 
      service.insert(request);
      return Response.noContent().build();
   }
   
   @POST  
   @Path("/select")
   @ApiOperation(value = "Select rows command")
   public Response selet(DatabaseRequest request) { 
      QueryResult result = service.select(request);
      return Response.ok(result).build();
   }
   
}
