package tachyon.client;

import java.io.IOException;

abstract public class StorageBlockWriter {
  protected long mBlockId = 0L;
  
  public StorageBlockWriter(long blockid) {
    
  }
  
  /**
   * Write the output data onto the file under the current storage directory
   */
  public abstract int appendCurrentBuffer(long blockid, byte[] buf, int offset, int length)
      throws IOException;
  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
