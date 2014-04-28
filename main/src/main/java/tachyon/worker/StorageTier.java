package tachyon.worker;

import java.util.List;

import org.apache.thrift.TException;

import tachyon.StorageDir;
import tachyon.conf.WorkerConf;

/**
 * Hierarchy Storage Tier management
 * 
 */
public class StorageTier {
  private WorkerConf mWorkerConf = WorkerConf.get();
  private int mMaxStorageLevel = mWorkerConf.MAX_HIERARCHY_STORAGE_LEVEL;

  // The storage level for current storage tier
  public int mStorageLevel = 0;
  // The storage directory list for each storage level
  public StorageDir[] mStorageDirs = new StorageDir[mMaxStorageLevel];
  // The storage directory quota for each storage directory
  public int[] mStorageDirCapacities = new int[mMaxStorageLevel];
  // The free and used space size for each storage directory
  public int[] mStorageDirFreeSpace = new int[mMaxStorageLevel];
  public int[] mStorageDirUsedSpace = new int[mMaxStorageLevel];
  // The next level storage tier hierarchically
  public StorageTier mNextStorageTier = null;

  /**
   * The user request certain space from the current storage tier.
   * It will return back the affordable StorageDir according to strategy(either random or
   * round-robbin). Otherwise it returns null if no more space available in mStorageDirs.
   * 
   * @param userId
   * @param requestBytes
   * @return
   * @throws TException
   */
  public StorageDir requestSpace(long userId, long requestBytes) throws TException {
    return null;
  }
  
  /**
   * Find the storageDir for certain blockId
   * @param blockId
   * @return
   */
  public StorageDir getStorageDir(long blockId) {
    return null;
  }
  
  /**
   * Return the storage file for certain blockId
   * @param blockId
   * @return
   */
  public String getStorageFile(long blockId) {
    return null;
  }

  private long freeBlock(long blockId) {
    return 0;
  }

  public void freeBlocks(List<Long> blocks) {
    for (long blockId : blocks) {
      freeBlock(blockId);
    }
  }

  /**
   * Use LRU to evict certain blocks into next level. Or directly free corresponding space for last
   * storage level.
   * 
   * @param requestBytes
   * @return
   */
  public boolean storageTierEviction(long requestBytes) {
    // TODO add more elimination algorithm
    if (mNextStorageTier != null) {
      // to evict to the next level
    } else {
      // if last level storage tier, just free the storage space
    }
    return false;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
