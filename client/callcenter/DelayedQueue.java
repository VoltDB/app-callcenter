/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package callcenter;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

public class DelayedQueue<T> {

    final NavigableMap<Long, Object[]> delayed = new TreeMap<>();
    protected int m_size = 0;

    public void add(long readyTs, T value) {
        Object[] values = delayed.get(readyTs);
        if (values != null) {

            Object[] values2 = new Object[values.length + 1];
            values2[0] = value;
            for (int i = 0; i < values.length; i++) {
                values2[i + 1] = values[i];
            }
            values = values2;
        }
        else {
            values = new Object[] { value };
        }
        delayed.put(readyTs, values);
        m_size++;
    }

    public T nextReady(long systemCurrentTimeMillis) {
        if (delayed.size() == 0) {
            return null;
        }

        if (delayed.firstKey() > systemCurrentTimeMillis) {
            return null;
        }

        Entry<Long, Object[]> entry = delayed.pollFirstEntry();
        Object[] values = entry.getValue();

        @SuppressWarnings("unchecked")
        T value = (T) values[0];
        if (values.length > 1) {
            int prevLength = values.length;
            values = Arrays.copyOfRange(values, 1, values.length);
            assert(values.length == prevLength - 1);
            delayed.put(entry.getKey(), values);
        }

        m_size--;
        return value;
    }

    public T drain() {
        return nextReady(Long.MAX_VALUE);
    }

    public int size() {
        /*int delayedCount = 0;
        for (Entry<Long, Object[]> entry : delayed.entrySet()) {
            delayedCount += entry.getValue().length;
        }
        return delayedCount;*/

        return m_size;
    }


}
