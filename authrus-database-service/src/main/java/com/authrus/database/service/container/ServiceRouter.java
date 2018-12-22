package com.authrus.database.service.container;

import lombok.SneakyThrows;

import com.authrus.http.Request;
import com.authrus.http.Response;
import com.authrus.http.socket.service.PathRouter;
import com.authrus.http.socket.service.Router;
import com.authrus.http.socket.service.Service;

public class ServiceRouter extends ServiceRegistry implements Router {

   private final Router router;
   
   @SneakyThrows
   public ServiceRouter() {
      this.router = new PathRouter(services, null);
   }
  
   @Override
   public Service route(Request request, Response response) {
      return router.route(request, response);
   }
}
