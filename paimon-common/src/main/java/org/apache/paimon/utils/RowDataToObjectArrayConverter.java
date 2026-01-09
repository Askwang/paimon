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

package org.apache.paimon.utils;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.util.stream.IntStream;

/** Convert {@link InternalRow} to object array. */
public class RowDataToObjectArrayConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType partitionType;
    private final InternalRow.FieldGetter[] fieldGetters;

    public RowDataToObjectArrayConverter(RowType partitionType) {
        this.partitionType = partitionType;
        this.fieldGetters =
                IntStream.range(0, partitionType.getFieldCount())
                        .mapToObj(
                                i ->
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                partitionType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
    }

    public RowType partitionType() {
        return partitionType;
    }

    public int getArity() {
        return fieldGetters.length;
    }

    public GenericRow toGenericRow(InternalRow rowData) {
        return GenericRow.of(convert(rowData));
    }

    /**
     * {@link InternalRowPartitionComputer#generatePartValues(InternalRow)} 这个类的
     * fieldGetters[i].getFieldOrNull(rowData) 结果会根据是否是 legacyPartitionName 区分.
     */
    public Object[] convert(InternalRow rowData) {
        Object[] result = new Object[fieldGetters.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            result[i] = fieldGetters[i].getFieldOrNull(rowData);
        }
        return result;
    }
}
