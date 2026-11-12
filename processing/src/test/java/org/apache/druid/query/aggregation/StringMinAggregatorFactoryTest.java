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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.any.NilVectorAggregator;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.TestVectorColumnSelectorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringMinAggregatorFactoryTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final int MAX_STRING_BYTES = 100;

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();
  private StringMinAggregatorFactory target;
  private ColumnCapabilities capabilitiesDictEncoded;

  @BeforeEach
  public void setUp()
  {
    target = new StringMinAggregatorFactory(
        NAME,
        FIELD_NAME,
        null,
        MAX_STRING_BYTES,
        true,
        ExprMacroTable.nil()
    );
    capabilitiesDictEncoded = ColumnCapabilitiesImpl.createDefault()
                                                    .setType(ColumnType.STRING)
                                                    .setDictionaryEncoded(true);
  }

  @Test
  public void testSerde() throws Exception
  {
    String json = mapper.writeValueAsString(target);
    AggregatorFactory deserialized = mapper.readValue(json, AggregatorFactory.class);
    Assertions.assertEquals(target, deserialized);
    Assertions.assertEquals(target.hashCode(), deserialized.hashCode());
  }

  @Test
  public void testCacheKey()
  {
    byte[] cacheKey = target.getCacheKey();
    Assertions.assertNotNull(cacheKey);
    Assertions.assertEquals(AggregatorUtil.STRING_MIN_CACHE_TYPE_ID, cacheKey[0]);
  }

  @Test
  public void testTypes()
  {
    Assertions.assertEquals(ColumnType.STRING, target.getIntermediateType());
    Assertions.assertEquals(ColumnType.STRING, target.getResultType());
    Assertions.assertEquals(Integer.BYTES + MAX_STRING_BYTES, target.getMaxIntermediateSize());
  }

  @Test
  public void testWithName()
  {
    AggregatorFactory renamed = target.withName("newName");
    Assertions.assertEquals("newName", renamed.getName());
    Assertions.assertEquals(target.getFieldName(), ((StringMinAggregatorFactory) renamed).getFieldName());
    Assertions.assertEquals(target.getMaxStringBytes(), ((StringMinAggregatorFactory) renamed).getMaxStringBytes());
  }

  @Test
  public void testCanVectorize()
  {
    ColumnInspector nullInspector = name -> null;
    Assertions.assertTrue(target.canVectorize(nullInspector));

    ColumnInspector dictInspector = name -> capabilitiesDictEncoded;
    Assertions.assertTrue(target.canVectorize(dictInspector));

    ColumnInspector nonDictInspector = name -> ColumnCapabilitiesImpl.createDefault().setType(ColumnType.STRING);
    Assertions.assertFalse(target.canVectorize(nonDictInspector));

    StringMinAggregatorFactory exprFactory = new StringMinAggregatorFactory(
        NAME,
        null,
        "concat(a, b)",
        MAX_STRING_BYTES,
        true,
        ExprMacroTable.nil()
    );
    Assertions.assertFalse(exprFactory.canVectorize(dictInspector));
  }

  @Test
  public void testFactorizeVectorWithoutCapabilitiesReturnsNil()
  {
    TestVectorColumnSelectorFactory vectorFactory = new TestVectorColumnSelectorFactory();
    vectorFactory.addCapabilities(FIELD_NAME, null);
    VectorAggregator aggregator = target.factorizeVector(vectorFactory);
    Assertions.assertInstanceOf(NilVectorAggregator.class, aggregator);
  }
}

