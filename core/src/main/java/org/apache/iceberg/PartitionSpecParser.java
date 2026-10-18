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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.Pair;

public class PartitionSpecParser {
  private PartitionSpecParser() {}

  private static final String SPEC_ID = "spec-id";
  private static final String FIELDS = "fields";
  private static final String SOURCE_ID = "source-id";
  private static final String FIELD_ID = "field-id";
  private static final String TRANSFORM = "transform";
  private static final String NAME = "name";
  private static final String HISTORICAL_SOURCE_TYPES = "historical-source-types";

  // Internal task/broadcast payload only. The table and manifest spec JSON formats stay unchanged.
  static String toJsonWithHistoricalSourceTypes(PartitionSpec spec) {
    if (spec.historicalSourceTypes().isEmpty()) {
      return toJson(spec);
    }
    return JsonUtil.generate(
        generator -> {
          generator.writeStartObject();
          generator.writeFieldName("spec");
          toJson(spec, generator);
          generator.writeFieldName(HISTORICAL_SOURCE_TYPES);
          Schema sourceSchema =
              new Schema(
                  spec.historicalSourceTypes().entrySet().stream()
                      .sorted(Map.Entry.comparingByKey())
                      .map(
                          entry ->
                              Types.NestedField.optional(
                                  entry.getKey(), "source_" + entry.getKey(), entry.getValue()))
                      .collect(Collectors.toList()));
          SchemaParser.toJson(sourceSchema, generator);
          generator.writeEndObject();
        },
        false);
  }

  static PartitionSpec fromJsonWithHistoricalSourceTypes(Schema schema, String json) {
    return JsonUtil.parse(
        json,
        node -> {
          if (!node.has(HISTORICAL_SOURCE_TYPES)) {
            return fromJson(node).bindUnchecked(schema);
          }
          UnboundPartitionSpec unbound = fromJson(JsonUtil.get("spec", node));
          Schema sourceSchema = SchemaParser.fromJson(JsonUtil.get(HISTORICAL_SOURCE_TYPES, node));
          Map<Integer, Type> sourceTypes = Maps.newHashMap();
          sourceSchema.columns().forEach(field -> sourceTypes.put(field.fieldId(), field.type()));
          return unbound.bindUnchecked(schema).withHistoricalSourceTypes(sourceTypes);
        });
  }

  public static void toJson(PartitionSpec spec, JsonGenerator generator) throws IOException {
    toJson(spec.toUnbound(), generator);
  }

  public static String toJson(PartitionSpec spec) {
    return toJson(spec, false);
  }

  public static String toJson(PartitionSpec spec, boolean pretty) {
    return toJson(spec.toUnbound(), pretty);
  }

  public static void toJson(UnboundPartitionSpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SPEC_ID, spec.specId());
    generator.writeFieldName(FIELDS);
    toJsonFields(spec, generator);
    generator.writeEndObject();
  }

  public static String toJson(UnboundPartitionSpec spec) {
    return toJson(spec, false);
  }

  public static String toJson(UnboundPartitionSpec spec, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(spec, gen), pretty);
  }

  public static PartitionSpec fromJson(Schema schema, JsonNode json) {
    return fromJson(json).bind(schema);
  }

  public static UnboundPartitionSpec fromJson(JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse spec from non-object: %s", json);
    int specId = JsonUtil.getInt(SPEC_ID, json);
    UnboundPartitionSpec.Builder builder = UnboundPartitionSpec.builder().withSpecId(specId);
    buildFromJsonFields(builder, JsonUtil.get(FIELDS, json));
    return builder.build();
  }

  private static final Cache<Pair<Types.StructType, String>, PartitionSpec> SPEC_CACHE =
      Caffeine.newBuilder().weakValues().build();

  public static PartitionSpec fromJson(Schema schema, String json) {
    return SPEC_CACHE.get(
        Pair.of(schema.asStruct(), json),
        schemaJsonPair -> JsonUtil.parse(json, node -> PartitionSpecParser.fromJson(schema, node)));
  }

  static void toJsonFields(PartitionSpec spec, JsonGenerator generator) throws IOException {
    toJsonFields(spec.toUnbound(), generator);
  }

  static void toJsonFields(UnboundPartitionSpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (UnboundPartitionSpec.UnboundPartitionField field : spec.fields()) {
      generator.writeStartObject();
      generator.writeStringField(NAME, field.name());
      generator.writeStringField(TRANSFORM, field.transformAsString());
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      generator.writeNumberField(FIELD_ID, field.partitionId());
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  static String toJsonFields(PartitionSpec spec) {
    return JsonUtil.generate(gen -> toJsonFields(spec, gen), false);
  }

  static PartitionSpec fromJsonFields(Schema schema, int specId, JsonNode json) {
    UnboundPartitionSpec.Builder builder = UnboundPartitionSpec.builder().withSpecId(specId);
    buildFromJsonFields(builder, json);
    return builder.build().bind(schema);
  }

  static PartitionSpec fromJsonFields(Schema schema, int specId, String json) {
    return JsonUtil.parse(json, node -> PartitionSpecParser.fromJsonFields(schema, specId, node));
  }

  static PartitionSpec fromJsonFieldsUnchecked(Schema schema, int specId, String json) {
    return JsonUtil.parse(
        json,
        node -> {
          UnboundPartitionSpec.Builder builder = UnboundPartitionSpec.builder().withSpecId(specId);
          buildFromJsonFields(builder, node);
          return builder.build().bindUnchecked(schema);
        });
  }

  private static void buildFromJsonFields(UnboundPartitionSpec.Builder builder, JsonNode json) {
    Preconditions.checkArgument(
        json.isArray(), "Cannot parse partition spec fields, not an array: %s", json);

    Iterator<JsonNode> elements = json.elements();
    int fieldIdCount = 0;
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(
          element.isObject(), "Cannot parse partition field, not an object: %s", element);

      String name = JsonUtil.getString(NAME, element);
      String transform = JsonUtil.getString(TRANSFORM, element);
      int sourceId = JsonUtil.getInt(SOURCE_ID, element);

      // partition field ids are missing in old PartitionSpec, they always auto-increment from
      // PARTITION_DATA_ID_START
      if (element.has(FIELD_ID)) {
        builder.addField(transform, sourceId, JsonUtil.getInt(FIELD_ID, element), name);
        fieldIdCount++;
      } else {
        builder.addField(transform, sourceId, name);
      }
    }

    Preconditions.checkArgument(
        fieldIdCount == 0 || fieldIdCount == json.size(),
        "Cannot parse spec with missing field IDs: %s missing of %s fields.",
        json.size() - fieldIdCount,
        json.size());
  }
}
