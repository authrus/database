package com.authrus.database.service.content;

import java.io.File;

import com.authrus.http.Path;
import com.authrus.http.Request;
import com.authrus.http.Response;

import com.fasterxml.jackson.databind.ObjectMapper;

public class FileContentHandlerMatcher implements ContentHandlerMatcher {

   private final ContentTypeResolver resolver;
   private final FileContentManager manager;
   private final ObjectMapper mapper;
   
   public FileContentHandlerMatcher(File root) {
      this.manager = new FileContentManager(root);
      this.resolver = new ContentTypeResolver();
      this.mapper = new ObjectMapper();
   }
   
   @Override
   public ContentHandler match(Request request, Response response) {
      Path path = request.getPath();
      String relative = path.getPath();
      FileContent content = manager.getContent(relative);
      
      if(content != null) {
         String location = content.getPath();
         String type = resolver.resolveType(location);
         
         if(content.isDirectory()) {
            return new DirectoryContentHandler(resolver, mapper, content);
         }
         return new FileContentHandler(content, type);
      }
      return null;
   }

}