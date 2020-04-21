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

import java.util.function.BiFunction;

public class Utility {

	public static Object getValue(Row row, int ordinal, LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return row.getBoolean(ordinal);
			case TINYINT:
				return row.getByte(ordinal);
			case SMALLINT:
				return row.getShort(ordinal);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return row.getInt(ordinal);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return row.getLong(ordinal);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return row.getTimestamp(ordinal);
			case FLOAT:
				return row.getFloat(ordinal);
			case DOUBLE:
				return row.getDouble(ordinal);
			case CHAR:
				return row.getChar(ordinal);
			case VARCHAR:
				return row.getString(ordinal);
			case DECIMAL:
				return row.getDecimal(ordinal);
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}


	/**
	 * Returns an accessor for an `InternalRow` with given data type. The returned accessor
	 * actually takes a `SpecializedGetters` input because it can be generalized to other classes
	 * that implements `SpecializedGetters` (e.g., `ArrayData`) too.
	 */
	static BiFunction<Row, Integer, Object> getAccessor(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return Row::getBoolean;
			case TINYINT:
				return Row::getByte;
			case SMALLINT:
				return Row::getShort;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return Row::getInt;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return Row::getLong;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return Row::getTimestamp;
			case FLOAT:
				return Row::getFloat;
			case DOUBLE:
				return Row::getDouble;
			case CHAR:
				return Row::getChar;
			case VARCHAR:
				return Row::getString;
			case DECIMAL:
				return Row::getDecimal;
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}

}
