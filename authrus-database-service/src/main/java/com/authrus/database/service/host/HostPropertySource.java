package com.authrus.database.service.host;

import java.net.InetAddress;

import org.springframework.core.env.PropertySource;

public class HostPropertySource extends PropertySource {
   
   private static final String HOST_PROPERTY = "host.name";

   public HostPropertySource(String name) {
      super(name);
   }

   @Override
   public Object getProperty(String name) {
       try {
          if(name.equals(HOST_PROPERTY)) {
             InetAddress local = InetAddress.getLocalHost();
             String canonical = local.getCanonicalHostName();
          
             return canonical.toLowerCase();
          }
       } catch (Exception e) {
           logger.error("Error determining host", e);
       }
       return null;
   }
}
