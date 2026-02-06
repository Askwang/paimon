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

package org.apache.paimon.flink;

import it.unimi.dsi.fastutil.Hash;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** paimon flink test. */
public class AskwangJavaTest extends CatalogITCaseBase {

    @Test
    public void testListInsertIndex() {
        RowType rowType = RowType.of(new DataField(0, "c1", new IntType()),
                new DataField(1, "c2", new IntType()),
                new DataField(2, "c3", new IntType()));
        DataField c4 = new DataField(rowType.getFieldCount(), "c4", new IntType());

//        Move move = Move.after(c4.name(), "c1");
        Move move = Move.first(c4.name());

        Map<String, Integer> map = new HashMap<>();
        for (DataField field : rowType.getFields()) {
            map.put(field.name(), field.id());
        }

        List<DataField> newFields = new ArrayList<>(rowType.getFields());
        if (move.type.equals(Move.MoveType.FIRST)) {
            newFields.add(0, c4);
        } else if (move.type.equals(Move.MoveType.AFTER)) {
            int fieldIndex = map.get(move.referenceFieldName());
            newFields.add(fieldIndex + 1, c4);
        } else {
            throw new IllegalArgumentException("Unknown move type: " + move.type);
        }

        newFields.forEach(x -> System.out.println(x.fullInfoAskwnag()));

    }

    static class Move {

        public enum MoveType {
            FIRST,
            AFTER
        }

        public static Move first(String fieldName) {
            return new Move(fieldName, null, MoveType.FIRST);
        }

        public static Move after(String fieldName, String referenceFieldName) {
            return new Move(fieldName, referenceFieldName, MoveType.AFTER);
        }

        private final String fieldName;
        private final String referenceFieldName;
        private final MoveType type;

        public Move(String fieldName,
                String referenceFieldName,
                MoveType type) {
            this.fieldName = fieldName;
            this.referenceFieldName = referenceFieldName;
            this.type = type;
        }

        public String fieldName() {
            return fieldName;
        }

        public String referenceFieldName() {
            return referenceFieldName;
        }

        public MoveType type() {
            return type;
        }

    }

    @Test
    public void testIteratorRemove() {
        List<Integer> list = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));
        List<Integer> result = new ArrayList<>();
        Iterator<Integer> iter = list.iterator();
        // list.removeIf(next -> next >= 4);
        while (iter.hasNext()) {
            Integer next = iter.next();
            if (next >= 4) {
                iter.remove();
                result.add(next);
            }
        }
        System.out.println(list);
        System.out.println(result);
    }
}
