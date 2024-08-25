/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import jdk.internal.util.ArraysSupport;
import net.openhft.affinity.AffinityLock;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CalculateAverage_jey_1_7 {
    // java --add-exports java.base/jdk.internal.util=ALL-UNNAMED CalculateAverage_jey_1_5.java

    static final String FILE = "/home/j/development/1brc/measurements.txt";

    static class ResultRow {
        int min, max;
        long sum;
        double mean;
        long count;

        ResultRow(int val) {
            this.min = val;
            this.max = val;
            this.sum = val;
            this.count = 1;
        }

        void calculateMean() {
            long afterDivByMUL = this.sum / MUL;
            this.mean = Math.round(afterDivByMUL * 10.0 / this.count) / 10.0;
        }

        public String toString() {
            return Math.round(min * 10.0 / MUL) / 10.0 + "/" + mean + "/" + Math.round(max * 10.0 / MUL) / 10.0;
        }

    }

    static int MUL = 10000;

    public static int byteArrHashCode(byte[] a, int f, int to) {
        if (a == null) {
            return 0;
        }
        return switch (a.length) {
            case 0 -> 1;
            case 1 -> 31 + (int) a[0];
            default -> ArraysSupport.vectorizedHashCode(a, f, to, 1, ArraysSupport.T_BYTE);
        };
    }

    static class ByteArrWrapper {
        byte[] wrapped;
        int l;
        int hashCode;

        public ByteArrWrapper() {
        }

        public ByteArrWrapper(byte[] arr, int l) {
            this.wrapped = arr;
            this.l = l;
            this.hashCode = byteArrHashCode(wrapped, 0, l);
        }

        public void set(byte[] arr, int l) {
            this.wrapped = arr;
            this.l = l;
            this.hashCode = byteArrHashCode(wrapped, 0, l);

        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ByteArrWrapper that = (ByteArrWrapper) o;
            if (this.l != that.l) {
                return false;
            }
            return hashCode == that.hashCode && Arrays.equals(this.wrapped, 0, this.l, that.wrapped, 0, that.l);
        }
    }

    static int[] divArr = {1, 10, 100, 1000, 10000};

    static int getInt(byte[] arr, int s, int l) {
        int num = 0;
        int mul = 1;
        int dotIndex = l - 1;
        for (int i = s + l - 1; i >= s; i--) {
            byte curr = arr[i];
            if (curr == '.') {
                dotIndex = i;
                continue;
            }
            if ('-' == (char) curr) {
                num *= -1;
                break;
            }
            num += (curr - '0') * mul;
            mul *= 10;
        }
        int tensToBeDivided = s + l - dotIndex - 1;
        int toBeMultiplied = MUL / divArr[tensToBeDivided];
        return toBeMultiplied * num;
    }

    static byte[] tmp = null;

    static class ByteProcessor {

        byte[] buf = new byte[100];
        int bi = 0;
        ByteArrWrapper key = new ByteArrWrapper();
        HashMap<ByteArrWrapper, ResultRow> hMap = new HashMap<>();

        void process(byte[] arr, long len) {
            for (int i=0; i<len; i++) {
                byte x = arr[i];
                buf[bi++] = x;

                if (x == ';') {
                    key.set(buf, bi - 1);
                    continue;
                }
                if (x == '\n') {
                    int temp = getInt(buf, key.l + 1, bi - key.l - 2);
                    ResultRow existing = hMap.get(key);
                    if (existing == null) {
                        existing = new ResultRow(temp);
                        hMap.put(new ByteArrWrapper(Arrays.copyOf(buf, key.l), key.l), existing);
                    } else {
                        if (existing.min > temp) {
                            existing.min = temp;
                        }
                        if (existing.max < temp) {
                            existing.max = temp;
                        }
                        existing.sum += temp;
                        existing.count += 1;
                    }
                    bi = 0;
                }
            }
        }

        void print() throws IOException {
            Map<String, ResultRow> map = new TreeMap<>();
            for (var e : hMap.entrySet()) {
                var m = e.getValue();
                m.calculateMean();
                var c = e.getKey();
                var cS = new String(c.wrapped, StandardCharsets.UTF_8);
                map.put(cS, m);
            }
            System.out.println(map);
        }
    }

    static class MmapFile {

        MemorySegment segment;
        long cursor = 0;
        long fileLen = 0;
        Unsafe unsafe;


        MmapFile(String file) throws Exception {
            var raf = new RandomAccessFile(file, "r");
            var fileChan = raf.getChannel();
            this.fileLen = raf.length();
            this.segment = fileChan.map(FileChannel.MapMode.READ_ONLY, 0, raf.length(), Arena.global());
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);


        }

        long readNBytes(byte[] buf) {
            if (cursor >= fileLen) {
                return 0;
            }
            long rem = buf.length;
            if (rem > this.fileLen - cursor) {
                rem = this.fileLen - cursor;
            }
            long address = this.segment.address();
            long start = address + cursor;
            for (int i = 0; i < rem; i++) {
                buf[i] = unsafe.getByte(start + i);
//                buf[i] = this.segment.get(ValueLayout.OfByte.JAVA_BYTE, cursor + i);
            }
            cursor += rem;
            return rem;
        }

    }

    static int MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        int tries = 1;
        try (AffinityLock al = AffinityLock.acquireLock(1)) {
            for (int i = 0; i < tries; i++) {
                System.gc();
                long start = System.currentTimeMillis();
                var bp = new ByteProcessor();
                var mmapFile = new MmapFile(FILE);
                byte[] read = new byte[20*MB];
                while (true) {
                    long readL = mmapFile.readNBytes(read);
                    if (readL == 0) {
                        break;
                    }
                    bp.process(read, readL);
                }
                bp.print();
                long duration = System.currentTimeMillis() - start;
                System.out.println(duration);
            }
        }

    }
}
