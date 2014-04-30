package tachyon.client;

import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.StorageDir;

abstract public class StorageBlockWriter {
  protected final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  protected long mBlockId = 0L;
  protected StorageDir mStorageDir = null;

  public StorageBlockWriter(StorageDir storage, long blockid) {
    mBlockId = blockid;
    mStorageDir = storage;
  }

  /**
   * Write the output data onto the file under the current storage directory
   */
  public abstract int appendCurrentBuffer(byte[] buf, int offset, int length) throws IOException;
}
