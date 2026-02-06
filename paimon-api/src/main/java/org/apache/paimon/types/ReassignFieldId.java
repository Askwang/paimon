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

package org.apache.paimon.types;

import java.util.concurrent.atomic.AtomicInteger;

/** Reassign field id by given field id. */
public class ReassignFieldId extends DataTypeDefaultVisitor<DataType> {

    private final AtomicInteger fieldId;

    public ReassignFieldId(AtomicInteger fieldId) {
        this.fieldId = fieldId;
    }

    /**
     * 访问者模式，本质是将对象和操作逻辑进行分离
     * DataType 为操作的对象，ReassignFieldId extends DataTypeDefaultVisitor 为操作的行为.
     */
    public static DataType reassign(DataType input, AtomicInteger fieldId) {
        return input.accept(new ReassignFieldId(fieldId));
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return new ArrayType(arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MultisetType multisetType) {
        return new MultisetType(
                multisetType.isNullable(), multisetType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MapType mapType) {
        return new MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public DataType visit(RowType rowType) {
        RowType.Builder builder = RowType.builder(rowType.isNullable(), fieldId);
        rowType.getFields()
                .forEach(
                        f ->
                                builder.field(
                                        f.name(),
                                        // askwang-done: f.type() 本身就是 paimon DataType，为什么又要访问一遍
                                        // DataField 的 type 可能还是复杂类型，需要递归处理内部的 DataField，保证类型重分配的完整性
                                        f.type().accept(this),
                                        f.description(),
                                        f.defaultValue()));
        return builder.build();
    }

    @Override
    protected DataType defaultMethod(DataType dataType) {
        return dataType;
    }
}
