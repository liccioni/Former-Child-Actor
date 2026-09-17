package dev.actorframework.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link Journal} backed by one {@link RandomAccessFile}, kept open for the actor's entire
 * lifetime (TASK-601). Format: sequential records, each a 4-byte big-endian length header followed
 * by that many payload bytes, repeated to EOF — minimal, streamable, no external dependency.
 */
final class FileBackedJournal implements Journal {

  private final RandomAccessFile file;

  FileBackedJournal(RandomAccessFile file) {
    this.file = file;
  }

  @Override
  public void append(byte[] record) {
    try {
      file.seek(file.length());
      file.writeInt(record.length);
      file.write(record);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public List<byte[]> readAll() {
    try {
      List<byte[]> records = new ArrayList<>();
      file.seek(0);
      long length = file.length();
      while (file.getFilePointer() < length) {
        int recordLength = file.readInt();
        byte[] record = new byte[recordLength];
        file.readFully(record);
        records.add(record);
      }
      return records;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      file.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
