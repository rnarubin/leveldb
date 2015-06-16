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

package org.iq80.leveldb;

import java.nio.ByteBuffer;

public interface Compression
{
    /**
     * Compress the content of the {@code src} buffer, writing the compressed output to the {@code dst} buffer.
     * After compression, the {@code dst} buffer's limit should be at the end of the compressed data set, and its
     * position should retain its original value
     * 
     * @param src
     *            Buffer containing uncompressed data
     * @param dst
     *            Buffer for receiving compressed data
     * @return size of compressed data, in bytes
     */
    int compress(ByteBuffer src, ByteBuffer dst);

    /**
     * Uncompress the content of the {@code src} buffer, writing the uncompressed output to the {@code dst} buffer.
     * After decompression, the {@code dst} buffer's limit should be at the end of the uncompressed data set, and its
     * position should retain its original value
     * 
     * @param src
     *            Buffer containing compressed data
     * @param dst
     *            Buffer for receiving uncompressed data
     * @return size of uncompressed data, in bytes
     */
    int uncompress(ByteBuffer src, ByteBuffer dst);

    /**
     * @param compressed
     *            buffer of compressed data
     * @return maximum size of a buffer yielded from decompressing the given buffer, in bytes. the buffer's fields should be unchanged
     */
    int maxUncompressedLength(ByteBuffer compressed);

    /**
     * @param compressed
     *            buffer of uncompressed data
     * @return maximum size of a buffer yielded from compressing the given buffer, in bytes. the buffer's fields should be unchanged
     */
    int maxCompressedLength(ByteBuffer uncompressed);

    /**
     * Byte value to be stored with serialized data, used to identify the compression implementation for decompression
     * 
     * <p/>
     * NOTE: <br>
     * value {@code 0} may not be used; it is reserved for non-compressed data. <br>
     * value {@code 1} is used by Snappy, enabled by default. User-specified compression may use the value {@code 1}
     * if Snappy compression was never enabled, or if the specified compression is a compatible Snappy decompressor
     * 
     * @return Unique compression identifier between {@code 0x00} and {@code 0xFF}
     */
    byte persistentId();
}
