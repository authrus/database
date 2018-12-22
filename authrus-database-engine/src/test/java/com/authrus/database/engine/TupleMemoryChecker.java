package com.authrus.database.engine;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.DecimalFormat;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.MBeanServer;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.attribute.AttributeSerializer;
import com.authrus.database.attribute.ObjectBuilder;
import com.authrus.database.bind.table.attribute.RowBuilder;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;

import com.bea.xml.stream.util.Stack;
import com.sun.management.HotSpotDiagnosticMXBean;

public class TupleMemoryChecker {
   
   // This is the name of the HotSpot Diagnostic MBean
   private static final String HOTSPOT_BEAN_NAME =
        "com.sun.management:type=HotSpotDiagnostic";

   // field to store the hotspot diagnostic MBean 
   private static volatile HotSpotDiagnosticMXBean hotspotMBean;

   private static final int ACCOUNTS = 1500000;

   public static void main(String[] list) throws Exception {
      ColumnSeries keys = new ColumnSeries();
      ColumnSeries columns = new ColumnSeries();
      PrimaryKey key = new PrimaryKey(keys);
      Properties properties = new Properties();
      Schema schema = new Schema(key, columns, properties);

      keys.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "key", "key", 0));

      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "key", "key", 0));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "name", "name", 1));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "mail", "mail", 2));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "password", "password", 3));
      columns.addColumn(new Column(DataConstraint.REQUIRED, DataType.TEXT, null, "type", "type", 4));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "rank", "rank", 5));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "percentile", "percentile", 6));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.DOUBLE, null, "rating", "rating", 7));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.DOUBLE, null, "deviation", "deviation", 8));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.DOUBLE, null, "volatility", "volatility", 9));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "blackGames", "blackGames", 10));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.INT, null, "whiteGames", "whiteGames", 11));
      columns.addColumn(new Column(DataConstraint.OPTIONAL, DataType.LONG, null, "lastSeenOnline", "lastSeenOnline", 12));

      ObjectBuilder builder = new RowBuilder();
      AttributeSerializer serializer = new AttributeSerializer(builder);
      RowMapper converter = new RowMapper(serializer, schema);      
      DecimalFormat format = new DecimalFormat("###,###,###,###.##");
      MemoryCounter counter = new MemoryCounter();
      Row[] tuples = new Row[ACCOUNTS];

      for (int i = 0; i < ACCOUNTS; i++) {
         tuples[i] = converter.createTuple(new Account(AccountType.PREMIUM, "key-" + i, "name-" + i, "mail-" + i, "password-" + i));
         
         if(i == 0) {
            System.err.println("Size of tuple is " + counter.estimate(tuples[i]));
         }        
      }
      Thread.sleep(1000);
      System.gc();

      double memoryLimit = Runtime.getRuntime().maxMemory();
      double memoryAllocated = Runtime.getRuntime().totalMemory();
      double memoryFree = Runtime.getRuntime().freeMemory();
      double memoryAvailable = memoryLimit - memoryAllocated;
      double memoryUsed = memoryLimit - (memoryFree + memoryAvailable);
      double percentageUsed = (memoryUsed / memoryLimit) * 100f;
      String percentage = Math.round(percentageUsed) + "%";

      System.err.println("Memory for " + ACCOUNTS + " is percentage=" + percentage + " memoryLimit=" + format.format(memoryLimit / (1024 * 1024)) + " megabytes " + " memoryUsed="
            + format.format(memoryUsed / (1024 * 1024)) + " megabytes memoryFree=" + format.format(memoryFree / (1024 * 1024)) + " megabytes size=" + tuples.length + " tuple="
            + format.format((memoryUsed / ACCOUNTS)) + " bytes");
   
      HeapDumper dumper = new HeapDumper();
      dumper.dumpHeap("c:\\temp\\heap.hprof", true);
      
      System.err.println("Memory for " + ACCOUNTS + " is percentage=" + percentage + " memoryLimit=" + format.format(memoryLimit / (1024 * 1024)) + " megabytes " + " memoryUsed="
            + format.format(memoryUsed / (1024 * 1024)) + " megabytes memoryFree=" + format.format(memoryFree / (1024 * 1024)) + " megabytes size=" + tuples.length + " tuple="
            + format.format((memoryUsed / ACCOUNTS)) + " bytes");
   }

   public static enum AccountType {
      ANONYMOUS, PREMIUM, ACTIVE, INACTIVE;
   }

   public static class Account implements Serializable {

      private static final long serialVersionUID = 1L;

      private AccountType type; // 16 bytes
      private String key; // 20 bytes
      private String name; // 20 bytes
      private String mail; // 20 bytes
      private String password; // 30 bytes
      private int rank; // 4 bytes
      private int percentile; // 4 bytes
      private double rating; // 8 bytes
      private double deviation; // 8 bytes
      private double volatility; // 8 bytes
      private int blackGames; // 4 bytes
      private int whiteGames; // 4 bytes
      private long lastSeenOnline; // 8 bytes // 170 bytes!!!

      public Account(AccountType type, String key, String name, String mail, String password) {
         this.key = key.toLowerCase();
         this.password = password;
         this.type = type;
         this.name = name;
         this.mail = mail;
      }

      public AccountType getType() {
         return type;
      }

      public void setType(AccountType type) {
         this.type = type;
      }

      public String getMail() {
         return mail;
      }

      public void setMail(String mail) {
         this.mail = mail;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getPassword() {
         return password;
      }

      public void setPassword(String password) {
         this.password = password;
      }

      public String getKey() {
         return key;
      }

      public void setKey(String userId) {
         this.key = userId;
      }

      public int getRank() {
         return rank;
      }

      public void setRank(int rank) {
         this.rank = rank;
      }

      public int getPercentile() {
         return percentile;
      }

      public void setPercentile(int percentile) {
         this.percentile = percentile;
      }

      public double getRating() {
         return rating;
      }

      public void setRating(double rating) {
         this.rating = rating;
      }

      public double getVolatility() {
         return volatility;
      }

      public void setVolatility(double volatility) {
         this.volatility = volatility;
      }

      public double getDeviation() {
         return deviation;
      }

      public void setDeviation(double deviation) {
         this.deviation = deviation;
      }

      public int getBlackGames() {
         return blackGames;
      }

      public void setBlackGames(int blackGames) {
         this.blackGames = blackGames;
      }

      public int getWhiteGames() {
         return whiteGames;
      }

      public void setWhiteGames(int whiteGames) {
         this.whiteGames = whiteGames;
      }

      public long getLastSeenOnline() {
         return lastSeenOnline;
      }

      public void setLastSeenOnline(long lastSeenOnline) {
         this.lastSeenOnline = lastSeenOnline;
      }

      @Override
      public String toString() {
         return String.format("name=%s id=%s", name, key);
      }
   }

   public static class MemorySizes {
      private final Map<Class, Integer> primitiveSizes = new IdentityHashMap<Class, Integer>() {
         {
            put(boolean.class, new Integer(1));
            put(byte.class, new Integer(1));
            put(char.class, new Integer(2));
            put(short.class, new Integer(2));
            put(int.class, new Integer(4));
            put(float.class, new Integer(4));
            put(double.class, new Integer(8));
            put(long.class, new Integer(8));
         }
      };

      public int getPrimitiveFieldSize(Class clazz) {
         return ((Integer) primitiveSizes.get(clazz)).intValue();
      }

      public int getPrimitiveArrayElementSize(Class clazz) {
         return getPrimitiveFieldSize(clazz);
      }

      public int getPointerSize() {
         return 4;
      }

      public int getClassSize() {
         return 8;
      }
   }

   public static class MemoryCounter {
      private static final MemorySizes sizes = new MemorySizes();
      private final Map visited = new IdentityHashMap();
      private final Stack stack = new Stack();

      public synchronized long estimate(Object obj) {
         assert visited.isEmpty();
         assert stack.isEmpty();
         
         long result = estimateSize(obj);
         while (!stack.isEmpty()) {
            result += estimateSize(stack.pop());
         }
         visited.clear();
         return result;
      }

      private boolean skipObject(Object obj) {
         if (obj instanceof String) {
            // this will not cause a memory leak since
            // unused interned Strings will be thrown away
            if (obj == ((String) obj).intern()) {
               return true;
            }
         }
         return (obj == null) || visited.containsKey(obj);
      }

      private long estimateSize(Object obj) {
         if (skipObject(obj)) {
            return 0;
         }
         visited.put(obj, null);
         long result = 0;
         Class clazz = obj.getClass();
         
         if (clazz.isArray()) {
            return estimateArray(obj);
         }
         while (clazz != null) {
            Field[] fields = clazz.getDeclaredFields();
            
            for (int i = 0; i < fields.length; i++) {
               if (!Modifier.isStatic(fields[i].getModifiers())) {
                  if (fields[i].getType().isPrimitive()) {
                     result += sizes.getPrimitiveFieldSize(fields[i].getType());
                  } else {
                     result += sizes.getPointerSize();
                     fields[i].setAccessible(true);
                     try {
                        Object toBeDone = fields[i].get(obj);
                        if (toBeDone != null) {
                           stack.add(toBeDone);
                        }
                     } catch (IllegalAccessException ex) {
                        assert false;
                     }
                  }
               }
            }
            clazz = clazz.getSuperclass();
         }
         result += sizes.getClassSize();
         return roundUpToNearestEightBytes(result);
      }

      private long roundUpToNearestEightBytes(long result) {
         if ((result % 8) != 0) {
            result += 8 - (result % 8);
         }
         return result;
      }

      protected long estimateArray(Object obj) {
         long result = 16;
         int length = Array.getLength(obj);
         if (length != 0) {
            Class arrayElementClazz = obj.getClass().getComponentType();
            if (arrayElementClazz.isPrimitive()) {
               result += length * sizes.getPrimitiveArrayElementSize(arrayElementClazz);
            } else {
               for (int i = 0; i < length; i++) {
                  result += sizes.getPointerSize() + estimateSize(Array.get(obj, i));
               }
            }
         }
         return result;
      }
   }
   
   public static class HeapDumper {

      static void dumpHeap(String fileName, boolean live) {
          // initialize hotspot diagnostic MBean
          initHotspotMBean();
          try {
              hotspotMBean.dumpHeap(fileName, live);
          } catch (RuntimeException re) {
              throw re;
          } catch (Exception exp) {
              throw new RuntimeException(exp);
          }
      }

      // initialize the hotspot diagnostic MBean field
      private static void initHotspotMBean() {
          if (hotspotMBean == null) {
              synchronized (HeapDumper.class) {
                  if (hotspotMBean == null) {
                      hotspotMBean = getHotspotMBean();
                  }
              }
          }
      }

      // get the hotspot diagnostic MBean from the
      // platform MBean server
      private static HotSpotDiagnosticMXBean getHotspotMBean() {
          try {
              MBeanServer server = ManagementFactory.getPlatformMBeanServer();
              HotSpotDiagnosticMXBean bean = 
                  ManagementFactory.newPlatformMXBeanProxy(server,
                  HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
              return bean;
          } catch (RuntimeException re) {
              throw re;
          } catch (Exception exp) {
              throw new RuntimeException(exp);
          }
      }
  }

}
