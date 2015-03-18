package io.framed;

import java.util.ArrayList;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;


public class BTableWriter {

    public static int VERSION = 0;

    private static final Double toDouble(Object o)
    throws IllegalArgumentException {
        if (o instanceof Double) {
          return (Double) o;
        } else if (o instanceof Long) {
            return ((Long) o).doubleValue();
        } else {
            throw new IllegalArgumentException(
                "Invalid input to toDouble: " + o.getClass().getName());
        }
    }

    private static ArrayList<Double> iter2Doubles(Iterable<Object> iter) {
        ArrayList<Double> ds = new ArrayList<Double>();
        for(Object o : iter) {
            ds.add(toDouble(o));
        }
        return ds;
    }

    private static final int numValues(ArrayList<Double> ds) {
        int count = 0;
        for(Double d : ds) {
          if(d != 0.0) { count++; }
        }
        return count;
    }

    // Write a single row directly to a FileChannel, prefixed with the number
    // of materialized values and pessimistically allocating for worse-case
    // dense rows
    private static final void chanWriteRow(FileChannel chan, Iterable<Object> row)
    throws IOException {
        ArrayList<Double> rowVals = iter2Doubles(row);
        ByteBuffer buf = ByteBuffer.allocate(4 + (8 * rowVals.size()));
        buf.clear();
        buf.putInt(numValues(rowVals));

        int idx = 0;
        for(Double d : rowVals) {
            if (d != 0.0) {
                buf.putInt(idx);
                buf.putDouble(d);
            }
            idx++;
        }

        buf.flip();
        while(buf.hasRemaining()) { chan.write(buf); }
    }

    public static final File write(File dest, String header,
                                   Iterable< Iterable<Object> > rows)
    throws IllegalArgumentException, IOException {
        FileChannel chan = new RandomAccessFile(dest, "rw").getChannel();

        // Write version + labels
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + (2 * header.length()));
        buf.clear();
        buf.putInt(VERSION);
        buf.putInt(header.length());
        for (int i=0; i<header.length(); i++){
            buf.putChar(header.charAt(i));
        }
        buf.flip();
        while(buf.hasRemaining()) { chan.write(buf); }

        // Write rows
        for(Iterable<Object> row : rows) {
            chanWriteRow(chan, row);
        }

        chan.close();
        return dest;
    }

}
