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

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class StringMinAggregationTest
{
  private static final Integer MAX_STRING_SIZE = 1024;
  private AggregatorFactory stringMinAggFactory;
  private AggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestObjectColumnSelector<String> valueSelector;
  private TestObjectColumnSelector objectSelector;

  private final String[] strings = {"3333", "1111", "2222", null, "4444"};
  private final String[] stringsWithNullFirst = {null, "3333", "1111", null};
  private final String[] stringsAllNull = {null, null};

  @BeforeEach
  public void setup()
  {
    stringMinAggFactory = new StringMinAggregatorFactory("billy", "nilly", MAX_STRING_SIZE);
    combiningAggFactory = stringMinAggFactory.getCombiningFactory();
    valueSelector = new TestObjectColumnSelector<>(strings);
    objectSelector = new TestObjectColumnSelector<>(strings);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(null);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testStringMinAggregator()
  {
    Aggregator agg = stringMinAggFactory.factorize(colSelectorFactory);

    for (int i = 0; i < strings.length; i++) {
      aggregate(agg);
    }

    String result = (String) agg.get();
    Assertions.assertEquals("1111", result);
  }

  @Test
  public void testStringMinAggregatorWithNullFirst()
  {
    valueSelector = new TestObjectColumnSelector<>(stringsWithNullFirst);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = stringMinAggFactory.factorize(colSelectorFactory);

    for (int i = 0; i < stringsWithNullFirst.length; i++) {
      aggregate(agg);
    }

    String result = (String) agg.get();
    Assertions.assertEquals("1111", result);
  }

  @Test
  public void testStringMinAggregatorAllNulls()
  {
    valueSelector = new TestObjectColumnSelector<>(stringsAllNull);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = stringMinAggFactory.factorize(colSelectorFactory);

    for (int i = 0; i < stringsAllNull.length; i++) {
      aggregate(agg);
    }

    String result = (String) agg.get();
    Assertions.assertNull(result);
    Assertions.assertTrue(agg.isNull());
  }

  @Test
  public void testStringMinAggregatorEmptyString()
  {
    final String[] withEmpty = {"3333", "", "1111"};
    valueSelector = new TestObjectColumnSelector<>(withEmpty);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = stringMinAggFactory.factorize(colSelectorFactory);

    for (int i = 0; i < withEmpty.length; i++) {
      aggregate(agg);
    }

    String result = (String) agg.get();
    Assertions.assertEquals("", result);
    Assertions.assertFalse(agg.isNull());
  }

  @Test
  public void testStringMinAggregatorTruncation()
  {
    final String[] longStrings = {"ZZZZZZ", "AAAAAAA"};
    valueSelector = new TestObjectColumnSelector<>(longStrings);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.replay(colSelectorFactory);

    AggregatorFactory factoryWithLimit = new StringMinAggregatorFactory("billy", "nilly", 4);
    Aggregator agg = factoryWithLimit.factorize(colSelectorFactory);

    for (int i = 0; i < longStrings.length; i++) {
      aggregate(agg);
    }

    String result = (String) agg.get();
    Assertions.assertEquals("AAAA", result);
  }

  @Test
  public void testStringMinAggregatorMultiValue()
  {
    final Object[] multiValues = {
        ImmutableList.of("CCCC", "AAAA"),
        ImmutableList.of("BBBB")
    };
    TestObjectColumnSelector<Object> mvSelector = new TestObjectColumnSelector<>(multiValues);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(mvSelector);
    EasyMock.replay(colSelectorFactory);

    AggregatorFactory mvFactory = new StringMinAggregatorFactory("billy", "nilly", null, 1024, true, null);
    Aggregator agg = mvFactory.factorize(colSelectorFactory);

    agg.aggregate();
    mvSelector.increment();
    agg.aggregate();
    mvSelector.increment();

    Assertions.assertEquals("AAAA", agg.get());

    // With aggregateMultipleValues = false
    TestObjectColumnSelector<Object> mvSelectorNoAgg = new TestObjectColumnSelector<>(multiValues);
    ColumnSelectorFactory colFactoryNoAgg = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colFactoryNoAgg.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colFactoryNoAgg.makeColumnValueSelector("nilly")).andReturn(mvSelectorNoAgg);
    EasyMock.replay(colFactoryNoAgg);

    AggregatorFactory noAggFactory = new StringMinAggregatorFactory("billy", "nilly", null, 1024, false, null);
    Aggregator aggNo = noAggFactory.factorize(colFactoryNoAgg);

    aggNo.aggregate();
    mvSelectorNoAgg.increment();
    aggNo.aggregate();
    mvSelectorNoAgg.increment();

    Assertions.assertEquals("BBBB", aggNo.get());
  }

  @Test
  public void testStringMinBufferAggregator()
  {
    BufferAggregator agg = stringMinAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[stringMinAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    for (int i = 0; i < strings.length; i++) {
      aggregate(agg, buffer, 0);
    }

    String result = (String) agg.get(buffer, 0);
    Assertions.assertEquals("1111", result);
  }

  @Test
  public void testStringMinBufferAggregatorWithNullFirst()
  {
    valueSelector = new TestObjectColumnSelector<>(stringsWithNullFirst);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.replay(colSelectorFactory);

    BufferAggregator agg = stringMinAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[stringMinAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    for (int i = 0; i < stringsWithNullFirst.length; i++) {
      aggregate(agg, buffer, 0);
    }

    String result = (String) agg.get(buffer, 0);
    Assertions.assertEquals("1111", result);
  }

  @Test
  public void testStringMinBufferAggregatorAllNulls()
  {
    valueSelector = new TestObjectColumnSelector<>(stringsAllNull);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.replay(colSelectorFactory);

    BufferAggregator agg = stringMinAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[stringMinAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    for (int i = 0; i < stringsAllNull.length; i++) {
      aggregate(agg, buffer, 0);
    }

    String result = (String) agg.get(buffer, 0);
    Assertions.assertNull(result);
    Assertions.assertTrue(agg.isNull(buffer, 0));
  }

  @Test
  public void testCombine()
  {
    Assertions.assertEquals("AAAA", stringMinAggFactory.combine("BBBB", "AAAA"));
    Assertions.assertEquals("AAAA", stringMinAggFactory.combine("AAAA", "BBBB"));
    Assertions.assertEquals("AAAA", stringMinAggFactory.combine("AAAA", null));
    Assertions.assertEquals("AAAA", stringMinAggFactory.combine(null, "AAAA"));
    Assertions.assertNull(stringMinAggFactory.combine(null, null));
  }

  @Test
  public void testComparator()
  {
    Comparator comparator = stringMinAggFactory.getComparator();
    Assertions.assertTrue(comparator.compare("AAAA", "BBBB") < 0);
    Assertions.assertTrue(comparator.compare("BBBB", "AAAA") > 0);
    Assertions.assertEquals(0, comparator.compare("AAAA", "AAAA"));
    Assertions.assertTrue(comparator.compare(null, "AAAA") < 0);
    Assertions.assertTrue(comparator.compare("AAAA", null) > 0);
    Assertions.assertEquals(0, comparator.compare(null, null));
  }

  private void aggregate(Aggregator agg)
  {
    agg.aggregate();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(BufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    valueSelector.increment();
    objectSelector.increment();
  }
}

