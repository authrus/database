package com.authrus.database.engine.io;

import com.authrus.database.engine.io.ByteArrayCompressor;

import junit.framework.TestCase;

public class ByteArrayCompressorTest extends TestCase {
   
   public void testCompressor() throws Exception{
      ByteArrayCompressor compressor = new ByteArrayCompressor(4);
      byte[] compressed = compressor.write("Hello World!".getBytes("UTF-8"));
      byte[] decompressed = compressor.read(compressed);
      
      assertEquals(new String(decompressed, "UTF-8"), "Hello World!");   
   }

}
