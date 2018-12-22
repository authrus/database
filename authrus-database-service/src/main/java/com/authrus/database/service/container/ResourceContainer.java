package com.authrus.database.service.container;

import static org.simpleframework.http.Method.CONNECT;
import static org.simpleframework.http.Protocol.DATE;
import static org.simpleframework.http.Protocol.SERVER;
import static org.simpleframework.http.Status.INTERNAL_SERVER_ERROR;
import static org.simpleframework.http.Status.OK;

import com.authrus.database.service.content.ContentHandler;
import com.authrus.database.service.content.ContentHandlerMatcher;

import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;

@Slf4j
@AllArgsConstructor
public class ResourceContainer implements Container {

   private final ContentHandlerMatcher matcher;
   private final Container container;
   private final String name;
   
   public void handle(Request request, Response response) {
      ContentHandler handler = matcher.match(request, response);        
      String method = request.getMethod();
      long time = System.currentTimeMillis();

      response.setValue(SERVER, name);
      response.setDate(DATE, time);
      
      if(handler != null) {
         try {
            response.setStatus(OK);
            handler.handle(request, response);
         } catch(Throwable cause) {
            response.setStatus(INTERNAL_SERVER_ERROR);
            log.info("Error handling request {}", request, cause);            
         } finally {
            try {
               if(!method.equals(CONNECT)) {
                  response.close();
               }
            } catch(IOException ignore) {
               log.info("Could not close response", ignore);
            }
         }
      } else {
         container.handle(request, response);
      }
   }
 
}
