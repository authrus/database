package com.authrus.database.engine.io.replicate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class RemoteSocket implements RemoteConnection {
   
   private final OutputStream output;
   private final InputStream input;
   private final Socket socket;
   
   public RemoteSocket(Socket socket) throws IOException {
      this.output = socket.getOutputStream();
      this.input = socket.getInputStream();
      this.socket = socket;
   }
   
   @Override
   public DataOutputStream getOutputStream() {
      return new DataOutputStream(output);
   }
   
   @Override
   public DataInputStream getInputStream() {
      return new DataInputStream(input);
   }
   
   @Override
   public String toString() {
      return String.valueOf(socket);
   }
}
