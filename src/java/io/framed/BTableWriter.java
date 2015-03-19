package io.framed;

import java.util.ArrayList;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;


public class BTableWriter {

    public static int VERSION = 0;
    public static String SEP = String.valueOf((char) 31); // ASCII unit sep

    private static final int numValues(Iterable<Object> row) {
        int count = 0;
        for (Object o : row) {
          if (((double) o) != 0.0) { count++; }
        }
        return count;
    }

    public static final File write(File dest, String header,
                                   Iterable< Iterable<Object> > rows)
    throws IllegalArgumentException, IOException {
        FileChannel chan = new RandomAccessFile(dest, "rw").getChannel();
        ByteBuffer buf;

        // Write version + labels w/ len prefix
        buf = ByteBuffer.allocate(4 + 4 + (2 * header.length()));
        buf.clear();
        buf.putInt(VERSION);
        buf.putInt(header.length());
        for (int i = 0; i < header.length(); i++){
            buf.putChar(header.charAt(i));
        }
        buf.flip();
        while (buf.hasRemaining()) {
            chan.write(buf);
        }

        // Write rows
        // Each row is prefixed for number of materialized values +
        // pessimistically allocated for worst-case dense row
        int ncols = header.split(SEP).length;
        buf = ByteBuffer.allocate(4 + (8 * ncols));

        for (Iterable<Object> row : rows) {
            buf.clear();
            buf.putInt(numValues(row));

            int idx = 0;
            for(Object o : row) {
                Double d = (double) o;
                if (d != 0.0) {
                    buf.putInt(idx);
                    buf.putDouble(d);
                }
                idx++;
            }

            buf.flip();
            while (buf.hasRemaining()) {
                chan.write(buf);
            }
        }

        chan.close();
        return dest;
    }

}
