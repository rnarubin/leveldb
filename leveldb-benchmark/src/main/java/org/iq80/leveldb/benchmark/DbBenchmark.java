/**
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
package org.iq80.leveldb.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.DbImpl;
import org.iq80.leveldb.util.ByteBufferCrc32;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.MemoryManagers;
import org.iq80.leveldb.util.Snappy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.benchmark.DbBenchmark.DBState.EXISTING;
import static org.iq80.leveldb.benchmark.DbBenchmark.DBState.FRESH;
import static org.iq80.leveldb.benchmark.DbBenchmark.Order.RANDOM;
import static org.iq80.leveldb.benchmark.DbBenchmark.Order.SEQUENTIAL;
import static org.iq80.leveldb.benchmark.DbBenchmark.Concurrency.SERIAL;
import static org.iq80.leveldb.benchmark.DbBenchmark.Concurrency.CONCURRENT;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;

public class DbBenchmark
{
    private boolean useExisting;
    private Integer writeBufferSize;
    private File databaseDir;
    private double compressionRatio;
    private long startTime;

    protected enum Order
    {
        SEQUENTIAL,
        RANDOM
    }

    enum DBState
    {
        FRESH,
        EXISTING
    }

    enum Concurrency
    {
       SERIAL,
       CONCURRENT
    }

    //    Cache cache_;
    private List<String> benchmarks;
    private DB db_;
    private final int num_;
    private int reads_;
    private final int valueSize;
    // private int heap_counter_;
    // private double last_op_finish_;
    private long bytes_;
    private String message_;
    private String post_message_;
    //    private Histogram hist_;
    private RandomGenerator gen_;
    private final Random rand_;

    protected int keySize = 16;
    private final int thread_count;

    // State kept for progress messages
    int done_;
    int next_report_;     // When to report next

    final DBFactory factory;

    @SuppressWarnings("unchecked")
    public DbBenchmark(Map<Flag, Object> flags) throws Exception
    {
        ClassLoader cl = DbBenchmark.class.getClassLoader();
        factory = (DBFactory) cl.loadClass(System.getProperty("leveldb.factory", "org.iq80.leveldb.impl.Iq80DBFactory")).newInstance();
        benchmarks = (List<String>) flags.get(Flag.benchmarks);
        num_ = (Integer) flags.get(Flag.num);
        reads_ = (Integer) (flags.get(Flag.reads) == null ? flags.get(Flag.num) : flags.get(Flag.reads));
        valueSize = (Integer) flags.get(Flag.value_size);
        writeBufferSize = (Integer) flags.get(Flag.write_buffer_size);
        compressionRatio = (Double) flags.get(Flag.compression_ratio);
        useExisting = (Boolean) flags.get(Flag.use_existing_db);
        thread_count = (Integer) flags.get(Flag.thread_count);
        keySize = (Integer) flags.get(Flag.key_size);
        bytes_ = 0;
        rand_ = new Random(301);


        databaseDir = new File((String) flags.get(Flag.db));

        // delete heap files in db
        for (File file : databaseDir.listFiles()) {
            if (file.getName().startsWith("heap-")) {
                file.delete();
            }
        }

        if (!useExisting) {
            destroyDb();
        }

        gen_ = new RandomGenerator(compressionRatio);
    }

    protected void run()
            throws IOException
    {
        printHeader();
        open();
        if (Boolean.parseBoolean(System.getProperty("leveldb.benchmark.pause", "false"))) {
            System.out.println("Press any key to begin");
            System.in.read();
        }

        for (String benchmark : benchmarks) {
            start();

            boolean known = true;

            if (benchmark.equals("fillseq")) {
                write(WriteOptions.make(), SEQUENTIAL, FRESH, num_, valueSize, 1);
            }
            else if (benchmark.equals("fillbatch")) {
                write(WriteOptions.make(), SEQUENTIAL, FRESH, num_, valueSize, 1000);
            }
            else if (benchmark.equals("fillrandom")) {
                write(WriteOptions.make(), RANDOM, FRESH, num_, valueSize, 1);
            }
            else if (benchmark.equals("fillrandomconcurrent")) {
                write(WriteOptions.make(), RANDOM, FRESH, CONCURRENT, num_, valueSize, 1);
            }
            else if (benchmark.equals("overwrite")) {
                write(WriteOptions.make(), RANDOM, EXISTING, num_, valueSize, 1);
            }
            else if (benchmark.equals("fillsync")) {
                write(WriteOptions.make().sync(true), RANDOM, FRESH, num_ / 1000, valueSize, 1);
            }
            else if (benchmark.equals("fill100K")) {
                write(WriteOptions.make(), RANDOM, FRESH, num_ / 1000, 100 * 1000, 1);
            }
            else if (benchmark.equals("readseq")) {
                readSequential();
            }
            else if (benchmark.equals("readreverse")) {
                readReverse();
            }
            else if (benchmark.equals("readrandom")) {
                readRandom();
            }
            else if (benchmark.equals("readhot")) {
                readHot();
            }
            else if (benchmark.equals("readrandomsmall")) {
                int n = reads_;
                reads_ /= 1000;
                readRandom();
                reads_ = n;
            }
            else if (benchmark.equals("compact")) {
                compact();
            }
            else if (benchmark.equals("crc32c")) {
                crc32c(4096, "(4k per op)");
            }
            else if (benchmark.equals("acquireload")) {
                acquireLoad();
            }
            else if (benchmark.equals("snappycomp")) {
                if( Snappy.available() ) {
                    snappyCompressDirectBuffer();
                }
            }
            else if (benchmark.equals("snappyuncomp")) {
                if( Snappy.available() ) {
                    snappyUncompressDirectBuffer();
                }
            }
            else if (benchmark.equals("snap-array")) {
                if (Snappy.available()) {
                    snappyCompressArray();
                }
            }
            else if (benchmark.equals("snap-direct")) {
                if (Snappy.available()) {
                    snappyCompressDirectBuffer();
                }
            }
            else if (benchmark.equals("unsnap-array")) {
                if( Snappy.available() ) {
                    snappyUncompressArray();
                }
            }
            else if (benchmark.equals("unsnap-direct")) {
                if( Snappy.available() ) {
                    snappyUncompressDirectBuffer();
                }
            }
            else if (benchmark.equals("heapprofile")) {
                heapProfile();
            }
            else if (benchmark.equals("stats")) {
                printStats();
            }
            else {
                known = false;
                System.err.println("Unknown benchmark: " + benchmark);
            }
            if (known) {
                stop(benchmark);
            }
        }
        db_.close();
    }

    private void printHeader()
            throws IOException
    {
        printEnvironment();
        System.out.printf("Keys:       %d bytes each\n", keySize);
        System.out.printf("Values:     %d bytes each (%d bytes after compression)\n",
                valueSize,
                (int) (valueSize * compressionRatio + 0.5));
        System.out.printf("Entries:    %d\n", num_);
        System.out.printf("RawSize:    %.1f MB (estimated)\n", ((keySize + valueSize) / 1048576.0) * num_);
        System.out.printf("FileSize:   %.1f MB (estimated)\n",
                (((keySize + valueSize * compressionRatio) / 1048576.0) * num_));
        printOptions();
        printWarnings();
        System.out.printf("------------------------------------------------\n");
    }

    void printOptions()
    {
        System.out.println(defaultOptions().toString().replace(", ", "\n"));
    }

    void printWarnings()
    {
        boolean assertsEnabled = false;
        assert assertsEnabled = true; // Intentional side effect!!!
        if (assertsEnabled) {
            System.out.printf("WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
        }

        // See if snappy is working by attempting to compress a compressible string
        String text = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
        byte[] compressedText = null;
        try {
            compressedText = Snappy.instance().compress(text);
        }
        catch (Exception ignored) {
        }
        if (compressedText == null) {
            System.out.printf("WARNING: Snappy compression is not enabled\n");
        }
        else if (compressedText.length > text.length()) {
            System.out.printf("WARNING: Snappy compression is not effective\n");
        }
    }

    void printEnvironment()
            throws IOException
    {
        System.out.printf("LevelDB:    %s\n", factory);

        System.out.printf("Date:       %tc\n", new Date());

        File cpuInfo = new File("/proc/cpuinfo");
        if (cpuInfo.canRead()) {
            int numberOfCpus = 0;
            String cpuType = null;
            String cacheSize = null;
            for (String line : CharStreams.readLines(com.google.common.io.Files.newReader(cpuInfo, UTF_8))) {
                ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on(':').omitEmptyStrings().trimResults().limit(2).split(line));
                if (parts.size() != 2) {
                    continue;
                }
                String key = parts.get(0);
                String value = parts.get(1);

                if (key.equals("model name")) {
                    numberOfCpus++;
                    cpuType = value;
                }
                else if (key.equals("cache size")) {
                    cacheSize = value;
                }
            }
            System.out.printf("CPU:        %d * %s\n", numberOfCpus, cpuType);
            System.out.printf("CPUCache:   %s\n", cacheSize);
        }
    }

    protected Options defaultOptions(){
        return Options.make();
    }

    private void open()
            throws IOException
    {
        Options options = defaultOptions();
        options.createIfMissing(!useExisting);
        // todo block cache
        if (writeBufferSize != null) {
            options.writeBufferSize(writeBufferSize);
        }
        db_ = factory.open(databaseDir, options);
    }

    private void start()
    {
        startTime = System.nanoTime();
        bytes_ = 0;
        message_ = null;
        // last_op_finish_ = startTime;
        // hist.clear();
        done_ = 0;
        next_report_ = 100;
    }

    private void stop(String benchmark)
    {
        long endTime = System.nanoTime();
        double elapsedSeconds = 1.0d * (endTime - startTime) / TimeUnit.SECONDS.toNanos(1);

        // Pretend at least one op was done in case we are running a benchmark
        // that does nto call FinishedSingleOp().
        if (done_ < 1) {
            done_ = 1;
        }

        if (bytes_ > 0) {
            String rate = String.format("%6.1f MB/s", (bytes_ / 1048576.0) / elapsedSeconds);
            if (message_ != null) {
                message_ = rate + " " + message_;
            }
            else {
                message_ = rate;
            }
        }
        else if (message_ == null) {
            message_ = "";
        }

        System.out.printf("%-12s : %11.5f micros/op;%s%s\n",
                benchmark,
                elapsedSeconds * 1e6 / done_,
                (message_ == null ? "" : " "),
                message_);
//        if (FLAGS_histogram) {
//            System.out.printf("Microseconds per op:\n%s\n", hist_.ToString().c_str());
//        }

        if (post_message_ != null) {
            System.out.printf("\n%s\n", post_message_);
            post_message_ = null;
        }

    }

    protected ByteBuffer keygen(Order order, int val){
       int k = (order == SEQUENTIAL) ? val : rand_.nextInt(num_);
        return ByteBuffer.wrap(formatNumber(k));
    }

    private void write(WriteOptions writeOptions, Order order, DBState state, int numEntries, int valueSize, int entries_per_batch) throws IOException
    {
       write(writeOptions, order, state, SERIAL, numEntries, valueSize, entries_per_batch);
    }
    private void write(final WriteOptions writeOptions, final Order order, final DBState state, final Concurrency concurrent, final int numEntries, final int valueSize, final int entries_per_batch)
            throws IOException
    {
        if (state == FRESH) {
            if (useExisting) {
                message_ = "skipping (--use_existing_db is true)";
                return;
            }
            db_.close();
            db_ = null;
            destroyDb();
            open();
            start(); // Do not count time taken to destroy/open
        }

        if (numEntries != num_) {
            message_ = String.format("(%d ops)", numEntries);
        }
        if(concurrent == SERIAL){
           for (int i = 0; i < numEntries; i += entries_per_batch) {
               WriteBatch batch = db_.createWriteBatch();
               for (int j = 0; j < entries_per_batch; j++) {
                   int k = (order == SEQUENTIAL) ? i + j : rand_.nextInt(num_);
                    ByteBuffer key = keygen(order, k);
                    int len = key.remaining();
                    batch.put(key, gen_.generate(valueSize));
                    bytes_ += valueSize + len;
                   finishedSingleOp();
               }
               db_.write(batch, writeOptions);
               batch.close();
           }
        }
        else{
           ExecutorService pool = Executors.newFixedThreadPool(thread_count);
           List<Future<Void>> work = new ArrayList<>();
           final int chunkSize = numEntries/thread_count;
           for(int t = 0, s = 0; t < thread_count; t++, s+=chunkSize){
              final int start = s, end = s+chunkSize;
              work.add(pool.submit(new Callable<Void>(){
                 public Void call() throws IOException
                 {
                     for (int i = start; i < end; i += entries_per_batch) {
                        WriteBatch batch = db_.createWriteBatch();
                        for (int j = 0; j < entries_per_batch; j++) {
                           int k = (order == SEQUENTIAL) ? i + j : ThreadLocalRandom.current().nextInt(num_);
                           ByteBuffer key = keygen(order, k);
                           int len = key.remaining();
                           batch.put(key, gen_.generate(valueSize));
                           bytes_ += valueSize + len;
                           if(start == 0)
                           {
                              //avoid contention in counting with a small cheat
                              finishedOps(thread_count);
                           }
                        }
                        db_.write(batch, writeOptions);
                        batch.close();
                     }
                     return null;
                 }
              }));
           }

           try{
              for(Future<Void> job:work){
                 try
                 {
                    job.get();
                 }
                 catch (InterruptedException | ExecutionException e)
                 {
                    throw new IOException(e);
                 }
              }
           }
           finally {
              pool.shutdown();
           }
        }
    }

    public byte[] formatNumber(long n)
    {
        Preconditions.checkArgument(n >= 0, "number must be positive");

        byte []slice = new byte[keySize];

        int i = slice.length-1;
        while (n > 0) {
            slice[i--] = (byte) ('0' + (n % 10));
            n /= 10;
        }
        while (i >= 0) {
            slice[i--] = '0';
        }
        return slice;
    }

    private void finishedSingleOp()
    {
       finishedOps(1);
    }

    private void finishedOps(int n)
    {
//        if (histogram) {
//            todo
//        }
       done_ += n;
        if (done_ >= next_report_) {

            if (next_report_ < 1000) {
                next_report_ += 100;
            }
            else if (next_report_ < 5000) {
                next_report_ += 500;
            }
            else if (next_report_ < 10000) {
                next_report_ += 1000;
            }
            else if (next_report_ < 50000) {
                next_report_ += 5000;
            }
            else if (next_report_ < 100000) {
                next_report_ += 10000;
            }
            else if (next_report_ < 500000) {
                next_report_ += 50000;
            }
            else {
                next_report_ += 100000;
            }
            System.out.printf("... finished %d ops%30s\r", done_, "");

        }

    }

    private void readSequential()
    {
        for (int loops = 0; loops < 5; loops++) {
            DBIterator iterator = db_.iterator();
            iterator.seekToFirst();
            for (int i = 0; i < reads_ && iterator.hasNext(); i++) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                bytes_ += entry.getKey().length + entry.getValue().length;
                finishedSingleOp();
            }
            Closeables.closeQuietly(iterator);
        }
    }

    private void readReverse()
    {
        for (int loops = 0; loops < 3; loops++) {
            DBIterator iterator = db_.iterator();
            iterator.seekToLast();
            for (int i = 0; i < reads_ && iterator.hasPrev(); i++) {
                Map.Entry<byte[], byte[]> entry = iterator.prev();
                bytes_ += entry.getKey().length + entry.getValue().length;
                finishedSingleOp();
            }
            Closeables.closeQuietly(iterator);
        }
    }

    protected byte[] readRandomKeygen(){
      return formatNumber(rand_.nextInt(num_));
    }

    private void readRandom()
    {
        for (int i = 0; i < reads_; i++) {
            byte[] key = readRandomKeygen();
            byte[] value = db_.get(key);
            Preconditions.checkNotNull(value, "db.get(%s) is null", new String(key, UTF_8));
            bytes_ += key.length + value.length;
            finishedSingleOp();
        }
    }

    private void readHot()
    {
        int range = (num_ + 99) / 100;
        for (int i = 0; i < reads_; i++) {
            byte[] key = formatNumber(rand_.nextInt(range));
            byte[] value = db_.get(key);
            bytes_ += key.length + value.length;
            finishedSingleOp();
        }
    }

    private void compact()
    {
        if(db_ instanceof DbImpl) {
            ((DbImpl)db_).flushMemTable();
            for (int level = 0; level < NUM_LEVELS - 1; level++) {
                ((DbImpl)db_).compactRange(level, ByteBuffer.wrap("".getBytes(UTF_8)), ByteBuffer.wrap("~".getBytes(UTF_8)));
            }
        }
    }

    private void crc32c(int blockSize, String message)
    {
        // Checksum about 500MB of data total
        byte[] data = new byte[blockSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = 'x';

        }

        long bytes = 0;
        int crc = 0;
        while (bytes < 1000 * 1048576) {
            ByteBufferCrc32 checksum = ByteBuffers.crc32();
            checksum.update(data, 0, blockSize);
            crc = ByteBuffers.maskChecksum(checksum.getIntValue());
            finishedSingleOp();
            bytes += blockSize;
        }
        System.out.printf("... crc=0x%x\r", crc);

        bytes_ = bytes;
        // Print so result is not dead
        message_ = message;
    }

    private void acquireLoad()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void snappyCompressArray()
    {
        snappyCompress(MemoryManagers.heap());
    }

    private void snappyCompressDirectBuffer()
    {
        snappyCompress(MemoryManagers.direct());
    }

    private void snappyCompress(MemoryManager memory)
    {
        ByteBuffer raw = ByteBuffers.copy(gen_.generate(defaultOptions().blockSize()), memory);
        ByteBuffer compressedOutput = memory.allocate(Snappy.instance().maxCompressedLength(raw));

        long produced = 0;

        // attempt to compress the block
        while (bytes_ < 1024 * 1048576) {  // Compress 1G
            int compressedSize = Snappy.instance().compress(raw, compressedOutput);
            raw.clear();
            compressedOutput.clear();
            bytes_ += raw.limit();
            produced += compressedSize;
            finishedSingleOp();
        }

        message_ = String.format("(output: %.1f%%)", (produced * 100.0) / bytes_);
    }

    private void snappyUncompressArray()
    {
        snappyUncompress(MemoryManagers.heap());
    }

    private void snappyUncompressDirectBuffer()
    {
        snappyUncompress(MemoryManagers.direct());
    }

    private void snappyUncompress(MemoryManager memory)
    {
        int inputSize = defaultOptions().blockSize();
        ByteBuffer raw = gen_.generate(inputSize);
        ByteBuffer compressedOutput = ByteBuffer.allocate(Snappy.instance().maxCompressedLength(raw));
        int compressedLength;

        compressedLength = Snappy.instance().compress(ByteBuffers.duplicate(raw),
                ByteBuffers.duplicate(compressedOutput));
        compressedOutput.limit(compressedLength);

        ByteBuffer uncompressedBuffer = memory.allocate(inputSize);
        ByteBuffer compressedBuffer = memory.allocate(compressedLength);
        compressedBuffer.put(compressedOutput).flip();

        // attempt to uncompress the block
        while (bytes_ < 1L * 1024 * 1048576) {  // Compress 1G
            uncompressedBuffer.clear();
            compressedBuffer.position(0);
            compressedBuffer.limit(compressedLength);
            Snappy.instance().uncompress(compressedBuffer, uncompressedBuffer);
            bytes_ += inputSize;

            finishedSingleOp();
        }
    }

    private void heapProfile()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void destroyDb()
            throws IOException
    {
        Closeables.closeQuietly(db_);
        db_ = null;
        Files.delete(Files.walkFileTree(databaseDir.toPath(), new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                Files.delete(file);
                return super.visitFile(file, attrs);
            }
        }));
    }

    private void printStats()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    public static void main(String[] args)
            throws Exception
    {
        Map<Flag, Object> flags = new EnumMap<Flag, Object>(Flag.class);
        for (Flag flag : Flag.values()) {
            flags.put(flag, flag.getDefaultValue());
        }
        for (String arg : args) {
            boolean valid = false;
            if (arg.startsWith("--")) {
                try {
                    ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on("=").limit(2).split(arg.substring(2)));
                    if (parts.size() != 2) {

                    }
                    Flag key = Flag.valueOf(parts.get(0));
                    Object value = key.parseValue(parts.get(1));
                    flags.put(key, value);
                    valid = true;
                }
                catch (Exception e) {
                }
            }

            if (!valid) {
                System.err.println("Invalid argument " + arg);
                System.exit(1);
            }

        }
        new DbBenchmark(flags).run();
    }


    protected enum Flag
    {
        // Comma-separated list of operations to run in the specified order
        //   Actual benchmarks:
        //      fillseq       -- write N values in sequential key order in async mode
        //      fillrandom    -- write N values in random key order in async mode
        //      overwrite     -- overwrite N values in random key order in async mode
        //      fillsync      -- write N/100 values in random key order in sync mode
        //      fill100K      -- write N/1000 100K values in random order in async mode
        //      readseq       -- read N times sequentially
        //      readreverse   -- read N times in reverse order
        //      readrandom    -- read N times in random order
        //      readhot       -- read N times in random order from 1% section of DB
        //      crc32c        -- repeated crc32c of 4K of data
        //      acquireload   -- load N*1000 times
        //   Meta operations:
        //      compact     -- Compact the entire DB
        //      stats       -- Print DB stats
        //      heapprofile -- Dump a heap profile (if supported by this port)
        benchmarks(ImmutableList.<String>of(
                "fillseq",
                "fillseq",
                "fillseq",
                "fillsync",
                "fillrandom",
                "overwrite",
                "fillseq",
                "readrandom",
                "readrandom",  // Extra run to allow previous compactions to quiesce
                "readseq",
                "readreverse",
                "compact",
                "readrandom",
                "readseq",
                "readreverse",
                "fill100K",
                "crc32c",
                "snappycomp",
                "snappyuncomp",
                "snap-array",
                "snap-direct",
                "unsnap-array",
                "unsnap-direct"
                // "acquireload"
        ))
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(value));
                    }
                },

        // Arrange to generate values that shrink to this fraction of
        // their original size after compression
        compression_ratio(0.5d)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Double.parseDouble(value);
                    }
                },

        // Print histogram of operation timings
        histogram(false)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Boolean.parseBoolean(value);
                    }
                },

        // If true, do not destroy the existing database.  If you set this
        // flag and also specify a benchmark that wants a fresh database, that
        // benchmark will fail.
        use_existing_db(false)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Boolean.parseBoolean(value);
                    }
                },

        // Number of key/values to place in database
        num(1000000)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Number of read operations to do.  If negative, do FLAGS_num reads.
        reads(null)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Size of each value
        value_size(100)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Size of each key
        key_size(16)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Number of bytes to buffer in memtable before compacting
        // (initialized to default value by "main")
        write_buffer_size(null)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Number of bytes to use as a cache of uncompressed data.
        // Negative means use default settings.
        cache_size(-1)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Maximum number of files to keep open at the same time (use default if == 0)
        open_files(0)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Use the db with the following name.
        db("/tmp/dbbench")
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return value;
                    }
                },
        // If running concurrently, how many parallel threads
        thread_count(8)
                {
                   @Override
                   public Object parseValue(String value)
                   {
                      return Integer.parseInt(value);
                   }
                };

        private final Object defaultValue;

        private Flag(Object defaultValue)
        {
            this.defaultValue = defaultValue;
        }

        public abstract Object parseValue(String value);

        public Object getDefaultValue()
        {
            return defaultValue;
        }
    }

    private static class RandomGenerator
    {
        private final ByteBuffer data;

        private RandomGenerator(double compressionRatio)
        {
            // We use a limited amount of data over and over again and ensure
            // that it is larger than the compression window (32KB), and also
            // large enough to serve all typical value sizes we want to write.
            Random rnd = new Random(301);
            data = ByteBuffer.allocate(1048576 + 100);
            while (data.position() < 1048576) {
                // Add a short fragment that is as compressible as specified
                // by FLAGS_compression_ratio.
                data.put(compressibleString(rnd, compressionRatio, 100));
            }
            data.rewind();
        }

        private ByteBuffer generate(int length)
        {
            return ByteBuffers.duplicateByLength(data, ThreadLocalRandom.current().nextInt(data.limit() - length),
                    length);
        }

        private static ByteBuffer compressibleString(Random rnd, double compressionRatio, int len)
        {
            int raw = (int) (len * compressionRatio);
            if (raw < 1) {
                raw = 1;
            }
            ByteBuffer rawData = generateRandomSlice(rnd, raw);

            // Duplicate the random data until we have filled "len" bytes
            ByteBuffer dst = ByteBuffer.allocate(len);
            while (dst.position() < len) {
                rawData.rewind().limit(Math.min(rawData.limit(), dst.remaining()));
                dst.put(rawData);
            }
            dst.flip();
            return dst;
        }

        private static ByteBuffer generateRandomSlice(Random random, int length)
        {
            ByteBuffer rawData = ByteBuffer.allocate(length);
            while (rawData.hasRemaining()) {
                rawData.put((byte) (' ' + random.nextInt(95)));
            }
            rawData.flip();
            return rawData;
        }

    }

}
