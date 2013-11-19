package com.pivotal.gpdbreplication.transport;

 
import java.io.IOException;
import java.io.OutputStream;

import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyCodec;

public class CompressedOutputStream extends OutputStream
{
    static final int             DEFAULT_BLOCK_SIZE = 32 * 1024; // Use 32kb for the default block size

    protected final OutputStream out;
    private final int            blockSize;
    private int                  cursor             = 0;
    protected byte[]             uncompressed;
    protected byte[]             compressed;
    
    /*FRED*/
    static boolean headerWrote=false;
    

    public CompressedOutputStream(OutputStream out) throws IOException {
        this(out, DEFAULT_BLOCK_SIZE);
    }

    /**
     * @param out
     * @param blockSize
     *            byte size of the internal buffer size
     * @throws IOException
     */
    public CompressedOutputStream(OutputStream out, int blockSize) throws IOException {
        this.out = out;
        this.blockSize = blockSize;
        uncompressed = new byte[blockSize];
        compressed = new byte[Snappy.maxCompressedLength(blockSize)];
        /*FRED
        writeHeader(); */
    }

    protected void writeHeader() throws IOException {
    	headerWrote=true;
        SnappyCodec.currentHeader().writeHeader(out);
    }

    /**
     * Writes len bytes from the specified byte array starting at offset off to
     * this output stream. The general contract for write(b, off, len) is that
     * some of the bytes in the array b are written to the output stream in
     * order; element b[off] is the first byte written and b[off+len-1] is the
     * last byte written by this operation.
     */
    /* (non-Javadoc)
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        rawWrite(b, off, len);
    }

    /**
     * Compress the input long array data
     * 
     * @param d
     *            input array
     * @param off
     *            offset in the array
     * @param len
     *            the number of elements in the array to copy
     * @throws IOException
     */
    public void write(long[] d, int off, int len) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        rawWrite(d, off * 8, len * 8);
    }

    /**
     * Compress the input double array data
     * 
     * @param f
     *            input array
     * @param off
     *            offset in the array
     * @param len
     *            the number of elements in the array to copy
     * @throws IOException
     */
    public void write(double[] f, int off, int len) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        rawWrite(f, off * 8, len * 8);
    }

    /**
     * Compress the input float array data
     * 
     * @param f
     *            input array
     * @param off
     *            offset in the array
     * @param len
     *            the number of elements in the array to copy
     * @throws IOException
     */
    public void write(float[] f, int off, int len) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        rawWrite(f, off * 4, len * 4);
    }

    /**
     * Compress the input int array data
     * 
     * @param f
     *            input array
     * @param off
     *            offset in the array
     * @param len
     *            the number of elements in the array to copy
     * @throws IOException
     */
    public void write(int[] f, int off, int len) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        rawWrite(f, off * 4, len * 4);
    }

    /**
     * Compress the input short array data
     * 
     * @param f
     *            input array
     * @param off
     *            offset in the array
     * @param len
     *            the number of elements in the array to copy
     * @throws IOException
     */
    public void write(short[] f, int off, int len) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        rawWrite(f, off * 2, len * 2);
    }

    /**
     * Compress the input array data
     * 
     * @param d
     * @throws IOException
     */
    public void write(long[] d) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        write(d, 0, d.length);
    }

    /**
     * Compress the input array data
     * 
     * @param f
     * @throws IOException
     */
    public void write(double[] f) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        write(f, 0, f.length);
    }

    /**
     * Compress the input array data
     * 
     * @param f
     * @throws IOException
     */
    public void write(float[] f) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        write(f, 0, f.length);
    }

    /**
     * Compress the input array data
     * 
     * @param f
     * @throws IOException
     */
    public void write(int[] f) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        write(f, 0, f.length);
    }

    /**
     * Compress the input array data
     * 
     * @param f
     * @throws IOException
     */
    public void write(short[] f) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        write(f, 0, f.length);
    }

    /**
     * Compress the raw byte array data.
     * 
     * @param array
     *            array data of any type (e.g., byte[], float[], long[], ...)
     * @param byteOffset
     * @param byteLength
     * @throws IOException
     */
    public void rawWrite(Object array, int byteOffset, int byteLength) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        for (int readBytes = 0; readBytes < byteLength;) {
            int copyLen = Math.min(uncompressed.length - cursor, byteLength - readBytes);
            Snappy.arrayCopy(array, byteOffset + readBytes, copyLen, uncompressed, cursor);
            readBytes += copyLen;
            cursor += copyLen;

            if (cursor >= uncompressed.length) {
                dump();
            }
        }
    }

    /**
     * Writes the specified byte to this output stream. The general contract for
     * write is that one byte is written to the output stream. The byte to be
     * written is the eight low-order bits of the argument b. The 24 high-order
     * bits of b are ignored.
     */
    /* (non-Javadoc)
     * @see java.io.OutputStream#write(int)
     */
    @Override
    public void write(int b) throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        if (cursor >= uncompressed.length) {
            dump();
        }
        uncompressed[cursor++] = (byte) b;
    }

    /* (non-Javadoc)
     * @see java.io.OutputStream#flush()
     */
    @Override
    public void flush() throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        dump();
        out.flush();
    }

    static void writeInt(OutputStream out, int value) throws IOException {
    	
        out.write((value >> 24) & 0xFF);
        out.write((value >> 16) & 0xFF);
        out.write((value >> 8) & 0xFF);
        out.write((value >> 0) & 0xFF);
    }

    static int readInt(byte[] buffer, int pos) {
        int b1 = (buffer[pos] & 0xFF) << 24;
        int b2 = (buffer[pos + 1] & 0xFF) << 16;
        int b3 = (buffer[pos + 2] & 0xFF) << 8;
        int b4 = buffer[pos + 3] & 0xFF;
        return b1 | b2 | b3 | b4;
    }

    protected void dump() throws IOException {
    	/*FRED*/
    	if (!headerWrote) writeHeader();

        if (cursor <= 0)
            return; // no need to dump

        // Compress and dump the buffer content
        int compressedSize = Snappy.compress(uncompressed, 0, cursor, compressed, 0);
        writeInt(out, compressedSize);
        out.write(compressed, 0, compressedSize);
        cursor = 0;
    }

    /**
     * close the stream
     */
    /* (non-Javadoc)
     * @see java.io.OutputStream#close()
     */
    @Override
    public void close() throws IOException {
        flush();

        super.close();
        out.close();
    }

}
