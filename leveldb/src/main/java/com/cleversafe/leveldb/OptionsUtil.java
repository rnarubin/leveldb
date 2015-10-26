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

package com.cleversafe.leveldb;

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

class OptionsUtil
{
    @SuppressWarnings("unchecked")
    static <T> T populateFromProperties(String prefix, T object)
    {
        return populateFromProperties(System.getProperties(), prefix, (Class<T>) object.getClass(), object);
    }

    static <T> T populateFromProperties(Properties properties, String prefix, Class<T> clazz, T object)
    {
        invokeParams(properties, object, methodMap(clazz, prefix));
        return object;
    }

    @SuppressWarnings("serial")
    private static final Map<Class<?>, Class<?>> primitiveToWrapper = new HashMap<Class<?>, Class<?>>()
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

    private static void invokeParams(Properties properties, Object target, Map<String, List<Method>> methodMap)
    {
        properties: for (final Entry<String, List<Method>> propAndMethod : methodMap.entrySet()) {
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
                            | NoSuchMethodException | SecurityException failedToParse) {
                        // failed to parse given argument as primitive/enum
                        // try to parse as factory method
                        if ("null".equals(splitArg[i])) {
                            args[i] = null;
                            continue;
                        }
                        try {
                            // Unfortunately, we have to rely on runtime type checking, so beware user error
                            int methodIndex = splitArg[i].lastIndexOf('.');
                            if (methodIndex < 0) {
                                // can't parse as class.method
                                break methods;
                            }
                            String methodName = splitArg[i].substring(methodIndex + 1);
                            String className = splitArg[i].substring(0, methodIndex);
                            args[i] = Class.forName(className).getDeclaredMethod(methodName).invoke(null);
                        }
                        catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException
                                | InvocationTargetException | NoSuchMethodException | SecurityException failedToMakeArg) {
                            // the given argument isn't usable, skip this method name
                            break methods;
                        }
                    }
                }

                try {
                    m.invoke(target, args);
                }
                catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    // failed to set option
                    break methods;
                }
            }
        }

    }

    private static Map<String, List<Method>> methodMap(Class<?> clazz, String paramPrefix)
    {
        Map<String, List<Method>> map = new HashMap<>();
        for (final Method m : clazz.getMethods()) {

            // only consider instance methods which require parameters
            if (!m.getDeclaringClass().equals(clazz) || m.getParameterTypes().length == 0
                    || Modifier.isStatic(m.getModifiers())) {
                continue;
            }

            final String propertyName = paramPrefix + m.getName();

            // future-proof for possible overloading
            List<Method> ms = map.get(propertyName);
            if (ms == null) {
                map.put(propertyName, ms = new ArrayList<>());
            }
            ms.add(m);
        }
        return map;
    }

    static <T> void copyFields(Class<T> clazz, T from, T to)
    {
        if (from == null || to == null)
            return;

        // avoid copy-paste errors and improve ease of maintenance with reflection
        // it's slower, but that's why the old constructors are private/deprecated and users should call Options.make()
        for (final Field f : clazz.getDeclaredFields()) {
            final int mods = f.getModifiers();
            if (Modifier.isFinal(mods) || Modifier.isStatic(mods)) {
                continue;
            }

            f.setAccessible(true);
            try {
                f.set(to, f.get(from));
            }
            catch (IllegalArgumentException | IllegalAccessException e) {
                throw new Error(e);
            }
        }

    }

    static String toString(Object opt)
    {
        Class<?> clazz = opt.getClass();
        StringBuilder sb = new StringBuilder(clazz.getSimpleName()).append(" [");
        for (final Field f : clazz.getDeclaredFields()) {
            final int mods = f.getModifiers();
            if (Modifier.isFinal(mods) || Modifier.isStatic(mods)) {
                continue;
            }
            f.setAccessible(true);
            sb.append(f.getName());
            sb.append('=');
            try {
                sb.append(f.get(opt));
            }
            catch (IllegalArgumentException | IllegalAccessException e) {
                throw new Error(e);
            }
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(']');
        return sb.toString();
    }
}
