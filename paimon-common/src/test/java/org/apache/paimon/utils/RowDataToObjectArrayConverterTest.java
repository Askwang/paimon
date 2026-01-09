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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** Test for {@link RowDataToObjectArrayConverter}. */
public class RowDataToObjectArrayConverterTest {

    @Test
    public void testGenericRowConvert() {
        RowType partitionType =
                RowType.of(
                        new DataField(0, "day", DataTypes.STRING()),
                        new DataField(0, "hour", DataTypes.INT()));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(partitionType);
        GenericRow genericRow = GenericRow.of(BinaryString.fromString("2025-12-28"), 20);
        Object[] convert = converter.convert(genericRow);
        System.out.println(Arrays.toString(convert));
    }

    @Test
    public void testBinaryRowConvert() {
        RowType partitionType =
                RowType.of(
                        new DataField(0, "day", DataTypes.STRING()),
                        new DataField(0, "hour", DataTypes.INT()));
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(partitionType);
        BinaryRow binaryRow = new BinaryRow(partitionType.getFieldCount());
        BinaryRowWriter rowWriter = new BinaryRowWriter(binaryRow);
        rowWriter.writeString(0, BinaryString.fromString("2025-12-28"));
        rowWriter.writeInt(1, 20);
        Object[] convert = converter.convert(binaryRow);
        System.out.println(Arrays.toString(convert));
    }
}
