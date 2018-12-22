package com.authrus.database.service.host;

import java.net.InetAddress;

import org.apache.commons.lang3.StringUtils;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class HostNameResolver {
   
   private final String override;  

   @SneakyThrows
   public String resolveHost() {
      if(StringUtils.isBlank(override)) {
         InetAddress address = InetAddress.getLocalHost();
         String canonical = address.getCanonicalHostName();
         
         return canonical.toLowerCase();
      }
      return override;      
   }
}
