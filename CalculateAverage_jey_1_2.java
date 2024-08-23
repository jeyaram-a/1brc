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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CalculateAverage_jey_1_2 {

    static final String FILE = "/home/j/development/1brc/measurements.txt";

    static class ResultRow {
        double min, max, sum, mean;
        long count;

        ResultRow(double val) {
            this.min = val;
            this.max = val;
            this.sum = val;
            this.count = 1;
        }

        void calculateMean() {
            this.mean = (Math.round(this.sum * 10.0) / 10.0) / this.count;
        }

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class Processor {

        Map<String, ResultRow> hMap = new HashMap<>();

        void process(Measurement m) {
            var city = m.city;
            var temp = m.measurement;
            ResultRow existing = hMap.get(city);
            if (existing == null) {
                existing = new ResultRow(temp);
                hMap.put(city, existing);
            }
            else {
                existing.min = Math.min(temp, existing.min);
                existing.max = Math.max(temp, existing.max);
                existing.sum += temp;
                existing.count += 1;
            }
        }

        void calculateMean() {
            for (var m : hMap.values()) {
                m.calculateMean();
            }
        }

        void print() {
            System.out.println(new TreeMap<>(this.hMap));
        }
    }

    static class ByteArrWrapper {
        byte[] wrapped;
        int hashCode;

        void setWrapped(byte[] wrapped) {
            this.wrapped = wrapped;
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
        String city;
        double measurement;
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
                    measurement.city = new String(buf, 0, bi - 1, StandardCharsets.UTF_8);
                    bi = 0;
                    continue;
                }
                if (x == '\n') {
                    measurement.measurement = Double.parseDouble(new String(buf, 0, bi - 1, StandardCharsets.UTF_8));
                    bi = 0;
                    processor.process(measurement);
                    measurement = new Measurement();
                }
            }
        }
    }

    static int MB = 1024 * 1024;

    public static void main(String[] args) throws IOException {
        List<Long> durations = new ArrayList<>(10);
        for (int i = 0; i < 1; i++) {
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
            bp.processor.calculateMean();
            bp.processor.print();
            long duration = System.currentTimeMillis() - start;
            durations.add(duration);
        }
        System.out.println(durations);
    }
}
