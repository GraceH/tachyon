package tachyon.master;

public class HierarchyStorageInfo {
  // How many levels in the current worker storage
  private int mLevelCount = 0;
  // Quota spaces for each StorageTier
  private long[] mCapacityBytes = null;
  // Used spaces for each StorageTier
  private long[] mUsedBytes = null;

  public HierarchyStorageInfo(int levelCount, long[] capacityBytes) {
    mLevelCount = levelCount;
    mCapacityBytes = capacityBytes;
  }

  private long _getAvailableBytes(int level) {
    return mCapacityBytes[level] - mUsedBytes[level];
  }

  public long[] getAvailableBytes() {
    long[] availableBytes = new long[mLevelCount];
    for (int i = 0; i < mLevelCount; i ++) {
      availableBytes[i] = _getAvailableBytes(i);
    }
    return availableBytes;
  }

  public long getAvailableBytes(int level) {
    if (levelCheck(level)) {
      return _getAvailableBytes(level);
    } else {
      return -1;
    }
  }

  public long[] getCapacityBytes() {
    return mCapacityBytes;
  }

  public long getCapacityBytes(int level) {
    if (levelCheck(level)) {
      return mCapacityBytes[level];
    } else {
      return -1;
    }
  }

  public long getTotalAvailableBytes() {
    int availableBytes = 0;
    for(int i =0; i < mLevelCount; i++) {
      availableBytes += _getAvailableBytes(i);
    }
    return availableBytes;
  }

  public long getTotalCapacityBytes() {
    int capacityBytes = 0;
    for (int i = 0; i < mLevelCount; i ++) {
      capacityBytes += mCapacityBytes[i];
    }
    return capacityBytes;
  }

  public long getTotalUsedBytes() {
    int usedBytes = 0;
    for (int i = 0; i < mLevelCount; i ++) {
      usedBytes += mUsedBytes[i];
    }
    return usedBytes;
  }

  public long[] getUsedBytes() {
    return mUsedBytes;
  }

  public long getUsedBytes(int level) {
    if (levelCheck(level)) {
      return mUsedBytes[level];
    } else {
      return -1;
    }
  }

  private Boolean levelCheck(int level) {
    if (level >= mLevelCount) {
      return false;
    } else {
      return true;
    }
  }

  public void updateUsedBytes(int level, long usedBytes) {
    if (levelCheck(level)) {
      mCapacityBytes[level] = usedBytes;
    }
  }

  public void updateUsedBytes(long[] usedBytes) {
    mUsedBytes = usedBytes;
  }
}
