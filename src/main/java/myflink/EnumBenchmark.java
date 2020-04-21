/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(8)
@Fork(2)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EnumBenchmark {

	@Benchmark
	public void testOrdinal() {
		RowKind kind1 = RowKind.INSERT;
		RowKind kind2 = RowKind.UPDATE_BEFORE;
		RowKind kind3 = RowKind.UPDATE_AFTER;
		RowKind kind4 = RowKind.DELETE;

		for (int i = 0; i < 100; i++) {
			byte b1 = (byte) kind1.ordinal();
			RowKind k1 = RowKind.values()[b1];
			print(k1);

			byte b2 = (byte) kind2.ordinal();
			RowKind k2 = RowKind.values()[b2];
			print(k2);

			byte b3 = (byte) kind3.ordinal();
			RowKind k3 = RowKind.values()[b3];
			print(k3);

			byte b4 = (byte) kind4.ordinal();
			RowKind k4 = RowKind.values()[b4];
			print(k4);
		}
	}

	@Benchmark
	public void testValue() {
		RowKind kind1 = RowKind.INSERT;
		RowKind kind2 = RowKind.UPDATE_BEFORE;
		RowKind kind3 = RowKind.UPDATE_AFTER;
		RowKind kind4 = RowKind.DELETE;

		for (int i = 0; i < 100; i++) {
			byte b1 = kind1.toByteValue();
			RowKind k1 = RowKind.fromByteValue(b1);
			print(k1);

			byte b2 = kind2.toByteValue();
			RowKind k2 = RowKind.fromByteValue(b2);
			print(k2);

			byte b3 = kind3.toByteValue();
			RowKind k3 = RowKind.fromByteValue(b3);
			print(k3);

			byte b4 = kind4.toByteValue();
			RowKind k4 = RowKind.fromByteValue(b4);
			print(k4);
		}
	}

	private void print(RowKind kind) {
	}

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
			.include(EnumBenchmark.class.getSimpleName())
			.output("/Users/wuchong/Workspace/Miscellaneous/my-benchmark/Benchmark.log")
			.build();
		new Runner(options).run();
	}
}