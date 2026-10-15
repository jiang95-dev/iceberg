/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionSet {
  @Test
  public void testUnusedSpecWithDroppedSource() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    PartitionSpec oldSpec =
        PartitionSpec.builderFor(schema)
            .identity("data")
            .build()
            .toUnbound()
            .bindUnchecked(schema.select("id"));
    PartitionSpec currentSpec =
        PartitionSpec.builderFor(schema.select("id")).identity("id").withSpecId(1).build();
    PartitionSet partitions =
        PartitionSet.create(
            ImmutableMap.of(oldSpec.specId(), oldSpec, currentSpec.specId(), currentSpec));
    StructLike partition = TestHelpers.Row.of(1L);

    Assert.assertTrue(partitions.isEmpty());
    Assert.assertFalse(partitions.contains(oldSpec.specId(), partition));
    Assert.assertTrue(partitions.add(currentSpec.specId(), partition));
    Assert.assertFalse(partitions.add(currentSpec.specId(), TestHelpers.Row.of(1L)));
    Assert.assertEquals(1, partitions.size());
    Assert.assertTrue(partitions.contains(currentSpec.specId(), TestHelpers.Row.of(1L)));
    Assert.assertEquals(Pair.of(currentSpec.specId(), partition), partitions.iterator().next());
    Assert.assertEquals("[id=1]", partitions.toString());
    Assert.assertTrue(partitions.remove(currentSpec.specId(), TestHelpers.Row.of(1L)));
    Assert.assertTrue(partitions.isEmpty());
  }
}
