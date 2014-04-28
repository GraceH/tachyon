package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract public class StorageBlockReader {
  protected long mBlockId = 0L;
  
  public StorageBlockReader(long blockid) {
    mBlockId = blockid;
  }
  /**
   * Read the input data from the file under the current storage directory
   */
  public abstract ByteBuffer readByteBuffer(long blockid, int offset, int length)
      throws IOException;

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
