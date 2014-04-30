package tachyon.client;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import tachyon.StorageDir;
import tachyon.util.CommonUtils;

public class StorageBlockWriterLocalFS extends StorageBlockWriter {
  private RandomAccessFile mLocalFile = null;
  private FileChannel mLocalFileChannel = null;
  private String mLocalFilePath = null;

  public StorageBlockWriterLocalFS(StorageDir storage, long blockid) throws IOException {
    super(storage, blockid);
    // TODO Auto-generated constructor stub
    mLocalFilePath = mStorageDir.getFilePath(mBlockId);
    mLocalFile = new RandomAccessFile(mLocalFilePath, "rw");
    mLocalFileChannel = mLocalFile.getChannel();
    // change the permission of the temporary file in order that the worker can move it.
    CommonUtils.changeLocalFileToFullPermission(mLocalFilePath);
    // use the sticky bit, only the client and the worker can write to the block
    CommonUtils.setLocalFileStickyBit(mLocalFilePath);
    LOG.info(mLocalFilePath + " was created!");
  }

  @Override
  public int appendCurrentBuffer(byte[] buf, int offset, int length) throws IOException {
    MappedByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, offset, length);
    out.put(buf, 0, length);
    return offset + length;
  }
}
