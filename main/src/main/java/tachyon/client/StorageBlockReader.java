package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.StorageDir;

abstract public class StorageBlockReader {
  protected final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  protected long mBlockId = 0L;
  protected StorageDir mStorageDir = null;

  public StorageBlockReader(StorageDir storage, long blockid) {
    mBlockId = blockid;
    mStorageDir = storage;
  }

  /**
   * Read the input data from the file under the current storage directory
   */
  public abstract ByteBuffer readByteBuffer(int offset, long length) throws IOException;
}
