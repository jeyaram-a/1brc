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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CalculateAverage_jey_1_3 {

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

    static class Processor {

        Map<ByteArrWrapper, ResultRow> hMap = new HashMap<>();

        void process(Measurement m) {
            var temp = m.measurement;
            ResultRow existing = hMap.get(m.wrap);
            if (existing == null) {
                existing = new ResultRow(temp);
                hMap.put(m.wrap, existing);
            }
            else {
                existing.min = Math.min(temp, existing.min);
                existing.max = Math.max(temp, existing.max);
                existing.sum += temp;
                existing.count += 1;
            }
        }

        void print() {
            Map<String, ResultRow> map = new TreeMap<>();
            for (var e : hMap.entrySet()) {
                var m = e.getValue();
                m.calculateMean();
                var c = e.getKey();
                var cS = new String(c.wrapped, 0, c.wrapped.length, StandardCharsets.UTF_8);
                map.put(cS, m);
            }
            System.out.println(map);

        }
    }

    static class ByteArrWrapper {
        byte[] wrapped;
        int hashCode;

        public ByteArrWrapper(byte[] arr) {
            this.wrapped = arr;
            this.hashCode = Arrays.hashCode(wrapped);
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
            return hashCode == that.hashCode && Objects.deepEquals(wrapped, that.wrapped);
        }
    }

    static class Measurement {
        ByteArrWrapper wrap;
        int measurement;
    }

    static class ByteProcessor {

        byte[] buf = new byte[100];
        int bi = 0;
        Measurement measurement = new Measurement();
        Processor processor = new Processor();

        void process(byte[] arr) {
            for (byte x : arr) {
                buf[bi++] = x;

                if (x == ';') {
                    measurement.wrap = new ByteArrWrapper(Arrays.copyOf(buf, bi - 1));
                    bi = 0;
                    continue;
                }
                if (x == '\n') {
                    var tM = Double.parseDouble(new String(buf, 0, bi - 1, StandardCharsets.UTF_8));
                    measurement.measurement = (int) (tM * MUL);
                    bi = 0;
                    processor.process(measurement);
                    measurement = new Measurement();
                }
            }
        }
    }

    static int MB = 1024 * 1024;

    public static void main(String[] args) throws IOException {
        int tries = 10;
        List<Long> time = new ArrayList<>(tries);
        for (int i = 0; i < tries; i++) {
            long start = System.currentTimeMillis();
            var bp = new ByteProcessor();

            try (var fis = new FileInputStream(FILE)) {
                while (true) {
                    byte[] read = fis.readNBytes(2 * MB);
                    if (read.length == 0) {
                        break;
                    }
                    bp.process(read);
                }

            }
            bp.processor.print();
            long duration = System.currentTimeMillis() - start;
            System.out.println(duration);
            time.add(duration);
            System.gc();
        }
        System.out.println(time);
    }
}
