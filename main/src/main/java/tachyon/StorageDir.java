package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.client.StorageBlockReader;
import tachyon.client.StorageBlockReaderLocalFS;
import tachyon.client.StorageBlockWriter;
import tachyon.client.StorageBlockWriterLocalFS;

/**
 * Storage directory management
 * 
 */
abstract public class StorageDir {
  public String mStorageDirName = null;

  public StorageBlockWriter getBlockWriter(StorageDir storage, long blockid) throws IOException {
    if (mStorageDirName.startsWith("hdfs://") || mStorageDirName.startsWith("s3://")
        || mStorageDirName.startsWith("s3n://")) {
      //TODO
      return null;
    } else {
      return new StorageBlockWriterLocalFS(storage, blockid);
    }
    
  }

  public StorageBlockReader getBlockReader(StorageDir storage, long blockid) {
    if (mStorageDirName.startsWith("hdfs://") || mStorageDirName.startsWith("s3://")
        || mStorageDirName.startsWith("s3n://")) {
      //TODO
      return null;
    } else {
      return new StorageBlockReaderLocalFS(storage, blockid);
    }
  }

  public String getFilePath(long blockid) {
    return mStorageDirName + Constants.PATH_SEPARATOR + blockid;
  }
  
  /**
   * Copy block files across different storage layers.
   * @param blockid
   * @param dst
   * @return
   * @throws IOException
   */
  private boolean _copyBlock(long blockid, StorageDir dst) throws IOException {
    StorageBlockReader sbr = getBlockReader(this, blockid);
    long len = getBlockLength(blockid);
    ByteBuffer bf = sbr.readByteBuffer(0, len);
    StorageBlockWriter sbw = dst.getBlockWriter(this, blockid);
    return sbw.appendCurrentBuffer(bf.array(), 0, (int)len) > 0;
  }
  
  //TODO move this function to another generic super class for both StorageBlockReader|Writer
  public long getBlockLength(long blockid) throws IOException {
    String blockfile = getFilePath(blockid);
    return UnderFileSystem.get(blockfile).getFileSize(blockfile);
  }
  
  public boolean existsBlock(long blockid) throws IOException {
    String blockfile = getFilePath(blockid);
    return UnderFileSystem.get(blockfile).exists(blockfile);
  }
  
  public boolean deleteBlock(long blockid) throws IOException {
    String blockfile = getFilePath(blockid);
    return UnderFileSystem.get(blockfile).delete(blockfile, true);
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
      _copyBlock(blockid, dst);
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
      _copyBlock(blockid, dst);
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
