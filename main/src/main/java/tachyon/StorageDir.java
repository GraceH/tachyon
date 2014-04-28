package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Storage directory management
 * 
 */
abstract public class StorageDir {
  public String mStorageDirName = null;

  /**
   * Write the output data onto the file under the current storage directory
   */
  public abstract int appendCurrentBuffer(long blockid, byte[] buf, int offset, int length)
      throws IOException;

  /**
   * Read the input data from the file under the current storage directory
   */
  public abstract ByteBuffer readByteBuffer(long blockid, int offset, int length)
      throws IOException;

  public String getFilePath(long blockid) {
    return mStorageDirName + Constants.PATH_SEPARATOR + blockid;
  }

  public boolean moveBlock(long blockid, StorageDir dst) throws IOException {
    boolean isCopySuccess = false;
    String srcFile = getFilePath(blockid);
    String dstFile = dst.getFilePath(blockid);
    UnderFileSystem srcUfs = UnderFileSystem.get(srcFile);
    UnderFileSystem dstUfs = UnderFileSystem.get(dstFile);
    if (srcUfs.getClass().equals(dstUfs.getClass())) {
      isCopySuccess = srcUfs.rename(srcFile, dstFile);
    } else {
      //TODO read from src and write to the dst.
    }
    return isCopySuccess && srcUfs.delete(srcFile, true);
  }
  
  public boolean copyBlock(long blockid, StorageDir dst) throws IOException {
    boolean isCopySuccess = false;
    String srcFile = getFilePath(blockid);
    String dstFile = dst.getFilePath(blockid);
    UnderFileSystem srcUfs = UnderFileSystem.get(srcFile);
    UnderFileSystem dstUfs = UnderFileSystem.get(dstFile);
    if (srcUfs.getClass().equals(dstUfs.getClass())) {
      isCopySuccess = srcUfs.rename(srcFile, dstFile);
    } else {
      //TODO read from src and write to the dst.
    }
    return isCopySuccess;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
