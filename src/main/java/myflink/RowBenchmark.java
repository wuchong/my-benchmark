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

import org.apache.flink.table.types.logical.LogicalType;

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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(8)
@Fork(2)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RowBenchmark {

	public static Row[] rows = new Row[] {
		Row.of(true, (byte) 1, (short) 2, 3, 4, 5L, Timestamp.valueOf(LocalDateTime.now()), 0.1F, 1.11D, 'a', "hello world", new BigDecimal("1.234567")),
		Row.of(false, (byte) 2, (short) 3, 4, 5, 6L, Timestamp.valueOf(LocalDateTime.now()), 1.1F, 2.11D, 'b', "hello world2", new BigDecimal("2.234567")),
		Row.of(true, (byte) 3, (short) 4, 5, 6, 7L, Timestamp.valueOf(LocalDateTime.now()), 2.1F, 3.11D, 'c', "hello world3", new BigDecimal("3.234567")),
		Row.of(false, (byte) 4, (short) 5, 6, 7, 8L, Timestamp.valueOf(LocalDateTime.now()), 3.1F, 4.11D, 'd', "hello world4", new BigDecimal("4.234567")),
		Row.of(true, (byte) 5, (short) 6, 7, 8, 9L, Timestamp.valueOf(LocalDateTime.now()), 4.1F, 5.11D, 'e', "hello world5", new BigDecimal("5.234567")),
		Row.of(false, (byte) 6, (short) 7, 8, 9, 10L, Timestamp.valueOf(LocalDateTime.now()), 5.1F, 6.11D, 'f', "hello world6", new BigDecimal("6.234567")),
		Row.of(true, (byte) 7, (short) 8, 9, 10, 11L, Timestamp.valueOf(LocalDateTime.now()), 6.1F, 7.11D, 'g', "hello world7", new BigDecimal("7.234567")),
		Row.of(false, (byte) 8, (short) 9, 10, 11, 12L, Timestamp.valueOf(LocalDateTime.now()), 7.1F, 8.11D, 'h', "hello world8", new BigDecimal("8.234567")),
		Row.of(true, (byte) 9, (short) 10, 11, 12, 13L, Timestamp.valueOf(LocalDateTime.now()), 8.1F, 9.11D, 'i', "hello world9", new BigDecimal("9.234567")),
	};

	public static LogicalType[] types = new LogicalType[] {
		BOOLEAN().getLogicalType(),
		TINYINT().getLogicalType(),
		SMALLINT().getLogicalType(),
		INT().getLogicalType(),
		DATE().getLogicalType(),
		BIGINT().getLogicalType(),
		TIMESTAMP().getLogicalType(),
		FLOAT().getLogicalType(),
		DOUBLE().getLogicalType(),
		CHAR(1).getLogicalType(),
		STRING().getLogicalType(),
		DECIMAL(10, 1).getLogicalType()
	};

	public static BiFunction<Row, Integer, Object>[] accessors = new BiFunction[types.length];

	static {
		for (int i = 0; i < types.length; i++) {
			accessors[i] = Utility.getAccessor(types[i]);
		}
	}

	@Benchmark
	public void testUtility() {
		for (Row row : rows) {
			for (int i = 0; i < types.length; i++) {
				print(Utility.getValue(row, i, types[i]));
			}
		}
	}

	@Benchmark
	public void testAccessor() {
		for (Row row : rows) {
			for (int i = 0; i < types.length; i++) {
				print(accessors[i].apply(row, i));
			}
		}
	}

	private void print(Object a) {
	}

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
			.include(RowBenchmark.class.getSimpleName())
			.output("/Users/wuchong/Workspace/Miscellaneous/flink-sql-demo/Benchmark.log")
			.build();
		new Runner(options).run();
	}
}