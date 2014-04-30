package tachyon.client;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import tachyon.StorageDir;

public class StorageBlockReaderLocalFS extends StorageBlockReader {

  public StorageBlockReaderLocalFS(StorageDir storage, long blockid) {
    super(storage, blockid);
  }

  @Override
  public ByteBuffer readByteBuffer(int offset, long length) throws IOException {
    RandomAccessFile localFile = new RandomAccessFile(mStorageDir.getFilePath(mBlockId), "r");

    long fileLength = localFile.length();
    String error = null;
    if (offset > fileLength) {
      error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
    }
    if (error == null && length != -1 && offset + length > fileLength) {
      error =
          String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
              length, fileLength);
    }
    if (error != null) {
      localFile.close();
      throw new IOException(error);
    }

    if (length == -1) {
      length = fileLength - offset;
    }

    FileChannel localFileChannel = localFile.getChannel();
    ByteBuffer buf = localFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
    localFileChannel.close();
    localFile.close();
    return buf;
  }
}
