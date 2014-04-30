package tachyon.worker;

import java.io.IOException;
import java.util.List;
import java.util.Random;

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
  public StorageDir requestSpace(long requestBytes) throws TException {
    Random rand = new Random(System.currentTimeMillis());
    int  n = rand.nextInt(mMaxStorageLevel);
    StorageDir availableDir = null;
    for(int i = n, j = 0 ; j < mMaxStorageLevel; i++, j++) {
      if(mStorageDirFreeSpace[i] > requestBytes) {
        availableDir = mStorageDirs[i];
        mStorageDirFreeSpace[i] += requestBytes;
        break;
      }
    }
    return availableDir;
  }

  /**
   * Find the storageDir for certain blockId
   * 
   * @param blockId
   * @return
   * @throws IOException
   */
  public StorageDir getStorageDir(long blockId) throws IOException {
    StorageDir foundDir = null;
    for (StorageDir dir : mStorageDirs) {
      if (dir.existsBlock(blockId)) {
        foundDir = dir;
      }
    }
    return foundDir;
  }

  /**
   * Return the storage file for certain blockId
   * 
   * @param blockId
   * @return
   * @throws IOException
   */
  public String getStorageFile(long blockId) throws IOException {
    StorageDir foundDir = getStorageDir(blockId);
    if (foundDir != null) {
      return foundDir.getFilePath(blockId);
    }
    return null;
  }

  private boolean freeBlock(long blockId) throws IOException {
    StorageDir foundDir = getStorageDir(blockId);
    if (foundDir != null) {
      return foundDir.deleteBlock(blockId);
    }
    return false;
  }

  public void freeBlocks(List<Long> blocks) throws IOException {
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
   * @throws IOException 
   * @throws TException 
   */
  public boolean storageTierEviction(long blockId) throws IOException, TException {
    // TODO add more elimination algorithm
    StorageDir foundDir = getStorageDir(blockId);
    if (mNextStorageTier != null) {
      // to evict to the next level
      long requestBytes = foundDir.getBlockLength(blockId);
      StorageDir dst = mNextStorageTier.requestSpace(requestBytes);
      foundDir.moveBlock(blockId, dst);
    } else {
      // if last level storage tier, just free the storage space
      foundDir.deleteBlock(blockId);
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
