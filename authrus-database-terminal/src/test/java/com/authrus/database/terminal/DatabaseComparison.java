package com.authrus.database.terminal;


public class DatabaseComparison {   
   
//   private static final int MASTER_LISTEN_PORT = 4455;
//   private static final int SLAVE_LISTEN_PORT = 4451;   
//   private static final int ROWS = 1000000;
//   
//   private static Database createPostgreSQL(File path) throws Exception { //5432
//      PGPoolingDataSource source = new PGPoolingDataSource();
//      
//      source.setDataSourceName("pool");
//      source.setServerName("localhost:5432");
//      source.setDatabaseName("test");
//      source.setUser("postgres");
//      source.setPassword("password12");
//      source.setMaxConnections(10);
//      
//      Map<String, String> translations = new HashMap<String, String>();
//      QueryCompiler compiler = new QueryCompiler(translations);      
//      
//      translations.put("optional", null);
//      translations.put("required", "not null");
//      translations.put("sequence", "default sequence");     
//      
//      Database database = new PoolDatabase(source, compiler);
//      ThreadFactory factory = new ThreadPoolFactory(PoolDatabase.class);
//      ThreadPool pool = new ThreadPool(factory, 1);
//      Runnable initiator = TerminalServer.createServer(database, null, pool, "C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\template",4237);
//      
//      initiator.run();
//      return database;
//   }   
//   /*
//   private static Database createSQLite(File path) throws Exception {
//      File libraryFile = new File("C:\\Work\\development\\bitbucket\\database\\zuooh-standard-database\\lib\\sqlite4java-win32-x86.dll");
//         
//      if(!libraryFile.exists()) {
//         throw new FileNotFoundException("Unable to find library " + libraryFile);
//      }
//      SQLite.setLibraryPath("C:\\Work\\development\\bitbucket\\database\\zuooh-standard-database\\lib");
//      SQLite.loadLibrary();
//      File file = new File(path, "database.db");
//      
//      Database database = new StandardDatabase(file.getCanonicalPath(), "C:\\Work\\development\\bitbucket\\database\\zuooh-standard-database\\lib");     
//      ThreadFactory factory = new ThreadPoolFactory(PoolDatabase.class);
//      ThreadPool pool = new ThreadPool(factory, 1);
//      TerminalAcceptor console = new TerminalAcceptor(database, null, pool, 4236);
//      
//      console.start();
//      return database;
//   } */  
//   
//   private static Database createH2(File path) throws Exception {
//      JdbcDataSource source = new JdbcDataSource();
//      
//      source.setUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false");
//      
//      Map<String, String> translations = new HashMap<String, String>();
//      QueryCompiler compiler = new QueryCompiler(translations);      
//      
//      translations.put("optional", null);
//      translations.put("required", "not null");
//      translations.put("sequence", "auto_increment");
//      translations.put("time", "current_timestamp");
//      translations.put("text", "varchar(100)");
//      translations.put("insert or ignore", "insert");  
//      
//      Database database = new PoolDatabase(source, compiler);
//      ThreadFactory factory = new ThreadPoolFactory(PoolDatabase.class);
//      ThreadPool pool = new ThreadPool(factory, 1);
//      Runnable initiator = TerminalServer.createServer(database, null, pool, "C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\template",4235);
//      
//      initiator.run();
//      return database;
//   }
//   
//   private static Database createMasterTuple(String origin, String path) throws Exception {
//      File directory = new File(path, origin);
//      
//      if(!directory.exists()) {
//         directory.mkdirs();
//      }
//      String dir = directory.getAbsolutePath();
//      FileLog log = new FileLog(dir, origin, 1024 * 1024 * 10, 10000);
//      ChangeListener listener = new ChangeLogPersister(log);
//      Catalog catalog = new Catalog(listener, origin);
//      ChangeServer server = new ChangeServer(dir, MASTER_LISTEN_PORT);
//      ChangeAssembler assembler = new CatalogAssembler(catalog);
//      ThreadFactory factory = new ThreadPoolFactory(ChangeScheduler.class);
//      ThreadPool pool = new ThreadPool(factory, 1);
//      ChangeScheduler executor = new ThreadPoolScheduler(assembler, pool);           
//      ChangeReplicator replicator = new ChangeReplicator(executor, origin, dir, "localhost", SLAVE_LISTEN_PORT);           
//      LocalDatabase store = new LocalDatabase(catalog, origin);
//      Runnable initiator = TerminalServer.createServer(store, catalog, pool, "C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\template",4233);
//      
//      log.start();
//      initiator.run();
//      server.start(); // enable replication
//      replicator.start(); // start replication/restoration      
//     
//      return store;
//   }
//   
//   private static Database createSlaveTuple(String origin, String path) throws Exception {
//      File directory = new File(path, origin);
//      
//      if(!directory.exists()) {
//         directory.mkdirs();
//      }
//      String dir = directory.getAbsolutePath();
//      FileLog log = new FileLog(dir, origin, 1024 * 1024 * 10, 10000);
//      ChangeListener listener = new ChangeLogPersister(log);
//      Catalog catalog = new Catalog(listener, origin);
//      ChangeServer server = new ChangeServer(dir, SLAVE_LISTEN_PORT);
//      ChangeAssembler assembler = new CatalogAssembler(catalog);
//      ThreadFactory factory = new ThreadPoolFactory(ChangeScheduler.class);
//      ThreadPool pool = new ThreadPool(factory, 1);
//      ChangeScheduler executor = new ThreadPoolScheduler(assembler, pool);    
//      ChangeReplicator replicator = new ChangeReplicator(executor, origin, dir, "localhost", MASTER_LISTEN_PORT);      
//      LocalDatabase store = new LocalDatabase(catalog, origin);
//      Runnable initiator = TerminalServer.createServer(store, catalog, pool, "C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\template", 4234);
//      
//      log.start();
//      initiator.run();
//      server.start(); // enable replication
//      replicator.start(); // start replication/restoration      
//   
//      return store;
//   }
//   
//   public static void createDatabase(Database database) throws Exception{     
//      Random random = new SecureRandom();      
//      DatabaseConnection connection = database.getConnection();
//      connection.executeStatement("drop table if exists test");
//      connection.executeStatement("create table test(id int not null default sequence, name text, address text, age int default 22, change date default time,  primary key(id))");
//      //connection.executeStatement("create table test(id integer primary key autoincrement, name text, address text, age integer default 22, change integer default current_timestamp)");
//      //connection.executeStatement("create table test(id integer primary key auto_increment, name text, address text, age integer default 22, change timestamp default current_timestamp)");
//      //connection.executeStatement("create table test(id serial primary key, name text, address text, age integer default 22, change timestamp default current_timestamp)");  
//      
//      for(int i = 0; i < ROWS; i++) {
//         int rand1 = random.nextInt(50000);
//         int rand2 = random.nextInt(50000);
//         int rand3 = random.nextInt(50000);
//         connection.executeStatement("insert into test (name, address) values ('name-"+rand1+"', 'address-"+rand2+"')");
//         
//         if(i % 10000 == 0){
//            double memoryLimit = Runtime.getRuntime().maxMemory();
//            double memoryAllocated = Runtime.getRuntime().totalMemory();
//            double memoryFree = Runtime.getRuntime().freeMemory();
//            double memoryAvailable = memoryLimit - memoryAllocated;
//            double memoryUsed = memoryLimit - (memoryFree + memoryAvailable);
//            double percentageUsed = (memoryUsed / memoryLimit) * 100f;
//            String percentage =  Math.round(percentageUsed) + "%";
//            
//            System.out.println("inserting " + i + " memory used " + percentage);
//         }
//      }
//      
//      connection.closeConnection();
//   }
//   
//   public static void main(String[] list) throws Exception {
//      String mode = "master";
//      if(list.length > 0) {
//         mode = list[0];
//      }
//      ConsoleAppender consoleAppender = new ConsoleAppender(); // create appender
//      PatternLayout logLayout = new PatternLayout("%d %p [%t] %C: %m%n");
//      
//      consoleAppender.setLayout(logLayout);
//      consoleAppender.setThreshold(Level.DEBUG);
//      consoleAppender.activateOptions();
//      
//      Logger.getRootLogger().addAppender(consoleAppender);
//      
//      File file = new File("C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\database");
//      
//     /* File[] fileList = file.listFiles();
//      for(File existingFile : fileList){
//         if(existingFile.isFile()) {
//            System.err.println("Deleting " + existingFile);
//            existingFile.delete();
//         }
//      }*/      
//      Database database = null;
//      
//      if(mode.equalsIgnoreCase("master")) {
//         database = createMasterTuple("master", file.getCanonicalPath());
//      } else if(mode.equalsIgnoreCase("slave")){
//         database = createSlaveTuple("slave", file.getCanonicalPath());
//      } else if(mode.equals("h2")){
//         database = createH2(file.getCanonicalFile());
//      } else if(mode.equals("sqlite")) {
//       //  database = createSQLite(file.getCanonicalFile());
//         throw new IllegalArgumentException("Unknown mode " + mode);
//      } else if(mode.equals("postgresql")) {
//         database = createPostgreSQL(file.getCanonicalFile());
//      } else {
//         throw new IllegalArgumentException("Unknown mode " + mode);
//      }
//      createDatabase(database);
//   }
}
