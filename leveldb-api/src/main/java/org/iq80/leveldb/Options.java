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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class Options
        implements Cloneable // shallow field-for-field Object.clone
{
    private static final Options DEFAULT_OPTIONS = new Options(null);
    static {
        readProperties(System.getProperties());
    }

    static void readProperties(Properties properties)
    {
        // possibly update DEFAULT_OPTIONS with provided properties

        final String optionsPrefix = "leveldb.options.";
        final Map<String, List<Method>> methodsByName = new HashMap<>();
        for (final Method m : Options.class.getMethods()) {

            // only consider instance methods which require parameters
            if (!m.getDeclaringClass().equals(Options.class) || m.getParameterTypes().length == 0
                    || Modifier.isStatic(m.getModifiers())) {
                continue;
            }

            final String propertyName = optionsPrefix + m.getName();

            // future-proof for possible overloading
            List<Method> ms = methodsByName.get(propertyName);
            if (ms == null) {
                methodsByName.put(propertyName, ms = new ArrayList<>());
            }
            ms.add(m);
        }

        @SuppressWarnings("serial")
        final Map<Class<?>, Class<?>> primitiveToWrapper = new HashMap<Class<?>, Class<?>>()
        {
            {
                put(int.class, Integer.class);
                put(boolean.class, Boolean.class);
                put(long.class, Long.class);
                put(float.class, Float.class);
                put(double.class, Double.class);
                put(char.class, Character.class);
                put(byte.class, Byte.class);
                put(short.class, Short.class);
                put(void.class, Void.class);
            }
        };

        properties: for (final Entry<String, List<Method>> propAndMethod : methodsByName.entrySet()) {
            final String arg = properties.getProperty(propAndMethod.getKey());
            if (arg == null) {
                continue properties;
            }

            final String[] splitArg = arg.split(",", -1);
            methods: for (final Method m : propAndMethod.getValue()) {
                final Class<?>[] paramTypes = m.getParameterTypes();
                if (paramTypes.length != splitArg.length) {
                    continue methods;
                }

                final Object[] args = new Object[paramTypes.length];
                for (int i = 0; i < args.length; i++) {
                    try {
                        Class<?> c = paramTypes[i];
                        args[i] = (c.isPrimitive() ? primitiveToWrapper.get(c) : c)
                                .getMethod("valueOf", String.class)
                                .invoke(null, splitArg[i]);
                    }
                    catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
                            | NoSuchMethodException | SecurityException e) {
                        // failed to parse given argument(s)
                        break methods;
                    }
                }

                try {
                    m.invoke(DEFAULT_OPTIONS, args);
                }
                catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    // failed to set option
                    break methods;
                }
            }
        }
    }

    /**
     * @deprecated use {@link Options#make()}
     */
    @Deprecated
    public Options()
    {
        this(DEFAULT_OPTIONS);
    }

    private Options(final Options that)
    {
        if (that == null)
            return;

        // avoid copy-paste errors and improve ease of maintenance with reflection
        // it's slower, but that's why it's private/deprecated and users should call Options.make()
        for (final Field f : Options.class.getDeclaredFields()) {
            final int mods = f.getModifiers();
            if (Modifier.isFinal(mods) || Modifier.isStatic(mods)) {
                continue;
            }

            f.setAccessible(true);
            try {
                f.set(this, f.get(that));
            }
            catch (IllegalArgumentException | IllegalAccessException e) {
                throw new Error(e);
            }
        }
    }

    public static Options make()
    {
        return copy(DEFAULT_OPTIONS);
    }

    public static Options copy(final Options other)
    {
        if (other == null)
            throw new IllegalArgumentException("copy target cannot be null");
        try {
            return (Options) other.clone();
        }
        catch (final CloneNotSupportedException notExpected) {
            return new Options(other);
        }
    }

    public static final int CPU_DATA_MODEL = Integer.getInteger("sun.arch.data.model");

    // We only use MMAP on 64 bit systems since it's really easy to run out of
    // virtual address space on a 32 bit system when all the data is getting mapped
    // into memory. If you really want to use MMAP anyways, use -Dleveldb.mmap=true
    // or set useMMap(boolean) to true
    public static final boolean USE_MMAP_DEFAULT = Boolean.parseBoolean(System.getProperty("leveldb.mmap", ""
            + (CPU_DATA_MODEL > 32)));

    private boolean createIfMissing = true;
    private boolean errorIfExists = false;
    private int writeBufferSize = 4 << 20;
    private int maxOpenFiles = 1000;
    private int blockRestartInterval = 16;
    private int blockSize = 4 * 1024;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private boolean verifyChecksums = true;
    private boolean paranoidChecks = false;
    private DBComparator comparator;
    private Logger logger = null;
    private long cacheSize = 0;
    private boolean throttleLevel0 = true;
    private IOImpl io = USE_MMAP_DEFAULT ? IOImpl.MMAP : IOImpl.FILE;

    static void checkArgNotNull(Object value, String name)
    {
        if (value == null) {
            throw new IllegalArgumentException("The " + name + " argument cannot be null");
        }
    }

    public boolean createIfMissing()
    {
        return createIfMissing;
    }

    public Options createIfMissing(boolean createIfMissing)
    {
        this.createIfMissing = createIfMissing;
        return this;
    }

    public boolean errorIfExists()
    {
        return errorIfExists;
    }

    public Options errorIfExists(boolean errorIfExists)
    {
        this.errorIfExists = errorIfExists;
        return this;
    }

    public int writeBufferSize()
    {
        return writeBufferSize;
    }

    public Options writeBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int maxOpenFiles()
    {
        return maxOpenFiles;
    }

    public Options maxOpenFiles(int maxOpenFiles)
    {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public int blockRestartInterval()
    {
        return blockRestartInterval;
    }

    public Options blockRestartInterval(int blockRestartInterval)
    {
        this.blockRestartInterval = blockRestartInterval;
        return this;
    }

    public int blockSize()
    {
        return blockSize;
    }

    public Options blockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public CompressionType compressionType()
    {
        return compressionType;
    }

    public Options compressionType(CompressionType compressionType)
    {
        checkArgNotNull(compressionType, "compressionType");
        this.compressionType = compressionType;
        return this;
    }

    public boolean verifyChecksums()
    {
        return verifyChecksums;
    }

    public Options verifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    public long cacheSize()
    {
        return cacheSize;
    }

    public Options cacheSize(long cacheSize)
    {
        this.cacheSize = cacheSize;
        return this;
    }

    public DBComparator comparator()
    {
        return comparator;
    }

    public Options comparator(DBComparator comparator)
    {
        this.comparator = comparator;
        return this;
    }

    /**
     * @deprecated use <a href="http://www.slf4j.org/">SLF4J</a> bindings
     */
    @Deprecated
    public Logger logger()
    {
        return logger;
    }

    /**
     * @deprecated use <a href="http://www.slf4j.org/">SLF4J</a> bindings
     */
    @Deprecated
    public Options logger(Logger logger)
    {
        this.logger = logger;
        return this;
    }

    public boolean paranoidChecks()
    {
        return paranoidChecks;
    }

    public Options paranoidChecks(boolean paranoidChecks)
    {
        this.paranoidChecks = paranoidChecks;
        return this;
    }

    public enum IOImpl
    {
        // could include SMR in the future
        MMAP, FILE
    }

    public Options ioImplementation(IOImpl impl)
    {
        this.io = impl;
        return this;
    }

    public IOImpl ioImplemenation()
    {
        return io;
    }

    public boolean throttleLevel0()
    {
        return throttleLevel0;
    }

    public Options throttleLevel0(boolean throttleLevel0)
    {
        this.throttleLevel0 = throttleLevel0;
        return this;
    }
}
