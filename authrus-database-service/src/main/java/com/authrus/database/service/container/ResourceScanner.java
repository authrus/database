package com.authrus.database.service.container;

import io.swagger.jaxrs.listing.SwaggerSerializers;

import java.util.Set;

import javax.ws.rs.Path;

import lombok.SneakyThrows;

import com.authrus.database.service.status.PingResource;
import com.authrus.database.service.swagger.SwaggerResource;

public class ResourceScanner {

   private static String RESOURCE_SUFFIX = "Resource";
   
   private final ClassPathScanner scanner;
   
   public ResourceScanner(String packages) {
      this.scanner = new AnnotationPresentScanner(Path.class, RESOURCE_SUFFIX, packages);
   }
   
   @SneakyThrows
   public Set<Class<?>> scan(boolean swagger) {
      Set<Class<?>> types = scanner.scan();
      
      types.add(PingResource.class);
      types.add(CrossOriginFilter.class);
      
      if(swagger) {
         types.add(SwaggerResource.class);
         types.add(SwaggerSerializers.class);
      } else {
         types.remove(SwaggerResource.class);
      }
      return types;
   }
}
