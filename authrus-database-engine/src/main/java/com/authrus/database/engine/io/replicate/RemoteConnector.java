package com.authrus.database.engine.io.replicate;

import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;

import com.authrus.database.engine.io.DataRecord;
import com.authrus.database.engine.io.DataRecordConsumer;
import com.authrus.database.engine.io.DataRecordIterator;

public class RemoteConnector {
   
   private final String host;
   private final int port;
   
   public RemoteConnector(String host, int port) {
      this.host = host;
      this.port = port;
   }      
   
   public Iterator<DataRecord> connect(Position position) throws IOException {
      Socket socket = new Socket(host, port);
      RemoteConnection connection = new RemoteSocket(socket);
      RemoteBlockConsumer consumer = new RemoteBlockConsumer(connection, position);
      DataRecordConsumer source = new DataRecordConsumer(consumer, Integer.MAX_VALUE);
      
      socket.setTcpNoDelay(true);
      socket.setSoTimeout(60000);
      
      return new DataRecordIterator(source);
   }
}