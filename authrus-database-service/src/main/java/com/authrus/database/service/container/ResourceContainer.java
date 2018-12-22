package com.authrus.database.service.container;

import static com.authrus.http.Method.CONNECT;
import static com.authrus.http.Protocol.DATE;
import static com.authrus.http.Protocol.SERVER;
import static com.authrus.http.Status.INTERNAL_SERVER_ERROR;
import static com.authrus.http.Status.OK;

import com.authrus.database.service.content.ContentHandler;
import com.authrus.database.service.content.ContentHandlerMatcher;

import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.authrus.http.Request;
import com.authrus.http.Response;
import com.authrus.http.core.Container;

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
