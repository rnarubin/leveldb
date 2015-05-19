/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.util;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Little Endian slice of a byte array.
 */
public final class Slice
        implements Comparable<Slice>
{
    private final ByteBuffer buffer;
    // private final byte[] data;
    private final int offset;
    private final int length;

    private int hash;

    public Slice(int length)
    {
        buffer = ByteBuffer.allocate(length).order(LITTLE_ENDIAN);
        this.offset = 0;
        this.length = length;
    }

    public Slice(ByteBuffer b)
    {
        this.buffer = b;
        this.offset = b.position();
        this.length = b.remaining();
    }

    public Slice(byte[] data)
    {
        this(data, 0, data.length);
    }

    public Slice(byte[] data, int offset, int length)
    {
        Preconditions.checkNotNull(data, "array is null");
        this.buffer = ByteBuffer.wrap(data).order(LITTLE_ENDIAN);
        this.offset = offset;
        this.length = length;
    }

    private ByteBuffer dup()
    {
        return this.buffer.duplicate().order(LITTLE_ENDIAN);
    }

    /**
     * Length of this slice.
     */
    public int length()
    {
        return this.length;
    }

    /**
     * Gets the array underlying this slice.
     */
    public byte[] getRawArray()
    {
        byte[] b = new byte[this.buffer.capacity()];
        ((ByteBuffer) dup().limit(b.length).position(0)).get(b);
        return b;
    }

    /**
     * Gets the offset of this slice in the underlying array.
     */
    public int getRawOffset()
    {
        return this.offset;
    }

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.capacity}
     */
    public byte getByte(int index)
    {
        return buffer.get(index);
    }

    /**
     * Gets an unsigned byte at the specified absolute {@code index} in this
     * buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.capacity}
     */
    public short getUnsignedByte(int index)
    {
        return (short) (getByte(index) & 0xFF);
    }

    /**
     * Gets a 16-bit short integer at the specified absolute {@code index} in
     * this slice.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.capacity}
     */
    public short getShort(int index)
    {
        return buffer.getShort(index);
    }

    /**
     * Gets a 32-bit integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.capacity}
     */
    public int getInt(int index)
    {
        return buffer.getInt(index);
    }

    /**
     * Gets a 64-bit long integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.capacity}
     */
    public long getLong(int index)
    {
        return buffer.getLong(index);
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param dstIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code dstIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.capacity}, or
     * if {@code dstIndex + length} is greater than
     * {@code dst.capacity}
     */
    public void getBytes(int index, Slice dst, int dstIndex, int length)
    {
        ByteBuffer dest = (ByteBuffer) dst.buffer.duplicate().position(dstIndex);
        ByteBuffer src = (ByteBuffer) dup().limit(index + length).position(index);
        dest.put(src);
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code dstIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.capacity}, or
     * if {@code dstIndex + length} is greater than
     * {@code dst.length}
     */
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        Preconditions.checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);
        ((ByteBuffer) dup().position(index)).get(destination, destinationIndex, length);
    }

    public byte[] getBytes()
    {
        return getBytes(this.offset, this.length);
    }

    public byte[] getBytes(int index, int length)
    {
        byte[] ret = new byte[length];
        getBytes(index, ret, 0, length);
        return ret;
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index} until the destination's position
     * reaches its limit.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + dst.remaining()} is greater than
     * {@code this.capacity}
     */
    public void getBytes(int index, ByteBuffer destination)
    {
        Preconditions.checkPositionIndex(index, this.length);
        destination.put((ByteBuffer) dup().position(index));
    }

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * specified absolute {@code index}.
     *
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than
     * {@code this.capacity}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        nope();
    }

    private static final void nope()
    {
        throw new UnsupportedOperationException("fuck this shit");
    }

    /**
     * Transfers this buffer's data to the specified channel starting at the
     * specified absolute {@code index}.
     *
     * @param length the maximum number of bytes to transfer
     * @return the actual number of bytes written out to the specified channel
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than
     * {@code this.capacity}
     * @throws java.io.IOException if the specified channel threw an exception during I/O
     */
    public int getBytes(int index, GatheringByteChannel out, int length)
            throws IOException
    {
        nope();
        return 0;
    }

    /**
     * Sets the specified 16-bit short integer at the specified absolute
     * {@code index} in this buffer.  The 16 high-order bits of the specified
     * value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.capacity}
     */
    public void setShort(int index, int value)
    {
        this.buffer.putShort(index, (short) value);
    }

    /**
     * Sets the specified 32-bit integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.capacity}
     */
    public void setInt(int index, int value)
    {
        this.buffer.putInt(index, value);
    }

    /**
     * Sets the specified 64-bit long integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.capacity}
     */
    public void setLong(int index, long value)
    {
        this.buffer.putLong(index, value);
    }

    /**
     * Sets the specified byte at the specified absolute {@code index} in this
     * buffer.  The 24 high-order bits of the specified value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.capacity}
     */
    public void setByte(int index, int value)
    {
        this.buffer.put(index, (byte) value);
    }

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index}.
     *
     * @param srcIndex the first index of the source
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code srcIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.capacity}, or
     * if {@code srcIndex + length} is greater than
     * {@code src.capacity}
     */
    public void setBytes(int index, Slice src, int srcIndex, int length)
    {
        ByteBuffer dest = (ByteBuffer) dup().position(index);
        dest.put((ByteBuffer) src.buffer.duplicate().limit(srcIndex + length).position(srcIndex));
    }

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code srcIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.capacity}, or
     * if {@code srcIndex + length} is greater than {@code src.length}
     */
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        ByteBuffer dest = (ByteBuffer) dup().position(index);
        dest.put(source, sourceIndex, length);
    }

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index} until the source buffer's position
     * reaches its limit.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + src.remaining()} is greater than
     * {@code this.capacity}
     */
    public void setBytes(int index, ByteBuffer source)
    {
        ((ByteBuffer) dup().position(index)).put(source);
    }

    /**
     * Transfers the content of the specified source stream to this buffer
     * starting at the specified absolute {@code index}.
     *
     * @param length the number of bytes to transfer
     * @return the actual number of bytes read in from the specified channel.
     * {@code -1} if the specified channel is closed.
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than {@code this.capacity}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        nope();
        return 0;
    }

    /**
     * Transfers the content of the specified source channel to this buffer
     * starting at the specified absolute {@code index}.
     *
     * @param length the maximum number of bytes to transfer
     * @return the actual number of bytes read in from the specified channel.
     * {@code -1} if the specified channel is closed.
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than {@code this.capacity}
     * @throws java.io.IOException if the specified channel threw an exception during I/O
     */
    public int setBytes(int index, ScatteringByteChannel in, int length)
            throws IOException
    {
        ByteBuffer dest = (ByteBuffer) dup().limit(index + length).position(index);
        int readBytes = 0;

        do {
            int localReadBytes;
            try {
                localReadBytes = in.read(dest);
            }
            catch (ClosedChannelException e) {
                localReadBytes = -1;
            }
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                else {
                    break;
                }
            }
            else if (localReadBytes == 0) {
                break;
            }
            readBytes += localReadBytes;
        }
        while (readBytes < length);

        return readBytes;
    }

    public int setBytes(int index, FileChannel in, int position, int length)
    {
        nope();
        return 0;
    }

    public Slice copySlice()
    {
        return copySlice(0, length);
    }

    /**
     * Returns a copy of this buffer's sub-region.  Modifying the content of
     * the returned buffer or this buffer does not affect each other at all.
     */
    public Slice copySlice(int index, int length)
    {
        ByteBuffer src = (ByteBuffer) dup().limit(index + length).position(index);
        Slice ret = new Slice(length);
        ret.buffer.put(src);
        return ret;
    }

    public byte[] copyBytes()
    {
        return copyBytes(0, length);
    }

    public byte[] copyBytes(int index, int length)
    {
        return getBytes(index, length);
    }

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks.
     */
    public Slice slice()
    {
        return slice(0, length);
    }

    /**
     * Returns a slice of this buffer's sub-region. Modifying the content of
     * the returned buffer or this buffer affects each other's content while
     * they maintain separate indexes and marks.
     */
    public Slice slice(int index, int length)
    {
        if (index == 0 && length == this.length) {
            return this;
        }
        return new Slice((ByteBuffer) dup().limit(index + length).position(index));
    }

    /**
     * Creates an input stream over this slice.
     */
    public SliceInput input()
    {
        return new SliceInput(this);
    }

    /**
     * Creates an output stream over this slice.
     */
    public SliceOutput output()
    {
        return new BasicSliceOutput(this);
    }

    /**
     * Converts this buffer's readable bytes into a NIO buffer.  The returned
     * buffer shares the content with this buffer.
     */
    public ByteBuffer toByteBuffer()
    {
        return toByteBuffer(0, length);
    }

    /**
     * Converts this buffer's sub-region into a NIO buffer.  The returned
     * buffer shares the content with this buffer.
     */
    public ByteBuffer toByteBuffer(int index, int length)
    {
        return (ByteBuffer) dup().limit(index + length).position(index);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Slice slice = (Slice) o;

        // do lengths match
        if (length != slice.length) {
            return false;
        }

        // if arrays have same base offset, some optimizations can be taken...
        if (offset == slice.offset && buffer == slice.buffer) {
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (buffer.get(offset + i) != slice.buffer.get(slice.offset + i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        int result = length;
        result = 31 * result + this.buffer.hashCode();
        if (result == 0) {
            result = 1;
        }
        hash = result;
        return hash;
    }

    /**
     * Compares the content of the specified buffer to the content of this
     * buffer.  This comparison is performed byte by byte using an unsigned
     * comparison.
     */
    public int compareTo(Slice that)
    {
        return FastByteComparisons.compareTo(getBytes(), this.offset, this.length, that.getBytes(), that.offset,
                that.length);
    }

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.
     */
    public String toString(Charset charset)
    {
        return toString(0, length, charset);
    }

    /**
     * Decodes this buffer's sub-region into a string with the specified
     * character set.
     */
    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }

        return Slices.decodeString(toByteBuffer(index, length), charset);
    }

    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "length=" + length() +
                ')';
    }
}
