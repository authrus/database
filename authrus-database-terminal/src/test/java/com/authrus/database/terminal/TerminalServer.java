package com.authrus.database.terminal;


public class TerminalServer {
   // http://localhost:4233/terminal/index.html
//   public static Runnable createServer(Database database, Catalog catalog, Executor executor, String path, int port) throws Exception {
//      ConsoleService terminal = new ConsoleService(database, catalog, executor);
//      ConsoleResource terminalResource = new ConsoleResource(terminal, "command", "date", "session");
//      StringResource statusResource = new StringResource("Everything is ok", "text/plain", "UTF-8", Status.OK);
//      StringResource missingResource = new StringResource("Could not find file", "text/plain", "UTF-8", Status.NOT_FOUND);
//      Map<String, String> types = new LinkedHashMap<String, String>();
//      types.put(".*.js", "application/javascript");
//      types.put(".*.css", "text/css");
//      types.put(".*.html", "text/html");
//      types.put(".*.txt", "text/plain");
//      File base = new File(path);
//      ContentTypeResolver typeResolver = new ContentTypeResolver(types);
//      FileManager fileManager = new FileManager(base);
//      FileResolver fileResolver = new FileResolver(fileManager, "index.html");
//      FileSystemResource fileResource = new FileSystemResource(fileResolver, typeResolver);
//      Map<String, Resource> resources = new LinkedHashMap<String, Resource>();
//      resources.put("/.*/query", terminalResource);
//      resources.put("/.*/status", statusResource);
//      resources.put("/.*", fileResource);
//      ResourceMatcher resourceMatcher = new RegularExpressionMatcher(resources);
//      Map<String, ThreadModel> models = new LinkedHashMap<String, ThreadModel>();
//      models.put("/.*/query", ThreadModel.ASYNCHRONOUS);
//      models.put("/.*", ThreadModel.SYNCHRONOUS);
//      ResourceFilter resourceFilter = new ThreadModelFilter(models);
//      Container resourceContainer = new ResourceFilterContainer(resourceFilter, resourceMatcher, missingResource);
//      WebServer terminalServer = new WebServer(resourceContainer, port);
//      
//      return new TerminalInitiator(terminalServer);
//   }
//   
//   private static class TerminalInitiator implements Runnable {
//      
//      private final WebServer server;
//      
//      public TerminalInitiator(WebServer server) {
//         this.server = server;
//      }
//
//      @Override
//      public void run() {
//         try {
//            server.start();
//         } catch(Exception e) {
//            throw new IllegalStateException("Could not start server", e);
//         }
//      }
//      
//   }
}
