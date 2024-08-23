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
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static java.util.stream.Collectors.groupingBy;

/**
 * I figured out it would be very hard to win the main competition of the One Billion Rows Challenge.
 * but I think this code has a good chance to win a special prize for the Ugliest Solution ever! :)
 *
 * Anyway, if you can make sense out of not exactly idiomatic Java code, and you enjoy pushing performance limits
 * then QuestDB - the fastest open-source time-series database - is hiring: https://questdb.io/careers/core-database-engineer/
 * <p>
 * <b>Credit</b>
 * <p>
 * I stand on shoulders of giants. I wouldn't be able to code this without analyzing and borrowing from solutions of others.
 * People who helped me the most:
 * <ul>
 * <li>Thomas Wuerthinger (thomaswue): The munmap() trick and work-stealing. In both cases, I shameless copy-pasted their code.
 *     Including SWAR for detecting new lines. Thomas also gave me helpful hints on how to detect register spilling issues.</li>
 * <li>Quan Anh Mai (merykitty): I borrowed their phenomenal branch-free parser.</li>
 * <li>Marko Topolnik (mtopolnik): I use a hashing function I saw in his code. It seems the produce good quality hashes
 *     and it's next-level in speed. Marko joined the challenge before me and our discussions made me to join too!</li>
 * <li>Van Phu DO (abeobk): I saw the idea with simple lookup tables instead of complicated bit-twiddling in their code first.</li>
 * <li>Roy van Rijn (royvanrijn): I borrowed their SWAR code and initially their hash code impl</li>
 * <li>Francesco Nigro (franz1981): For our online discussions about performance. Both before and during this challenge.
 *     Francesco gave me the idea to check register spilling.</li>
 * </ul>
 */
public class CalculateAverage_jey_1_1 {

    static final String FILE = "/home/j/development/1brc/measurements.txt";

    static class ResultRow {
        double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY, sum, mean;
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

        void process(String line) {
            String[] splits = line.split(";");
            String city = splits[0];
            double temp = Double.parseDouble(splits[1]);
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
            System.out.println(new TreeMap<String, ResultRow>(this.hMap));
        }
    }

    static int MB = 1024 * 1024;

    public static void main(String[] args) throws IOException {
        List<Long> time = new ArrayList<>(10);

        for (int i = 0; i < 1; i++) {
            long start = System.currentTimeMillis();
            var processor = new Processor();
            try (var br = new BufferedReader(new FileReader(FILE), 200 * MB)) {
                String line;
                while ((line = br.readLine()) != null) {
                    processor.process(line);
                }
            }
            processor.calculateMean();
            processor.print();
            long duration = System.currentTimeMillis() - start;
            time.add(duration);
        }
        System.out.println(time);
    }
}
