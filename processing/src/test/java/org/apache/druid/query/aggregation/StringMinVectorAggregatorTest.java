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

import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;

import static org.apache.druid.query.aggregation.StringMinVectorAggregator.NOT_FOUND_FLAG_VALUE;
import static org.mockito.ArgumentMatchers.anyInt;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class StringMinVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int MAX_STRING_BYTES = 32;
  private static final int BUFFER_SIZE = 1024;
  private static final int POSITION = 2;

  // Dictionary:
  // 0 -> null
  // 1 -> "apple"
  // 2 -> "banana"
  // 3 -> "cherryThisStringIsLongerThanThirtyTwoBytes"
  private static final String[] DICTIONARY = new String[]{
      null,
      "apple",
      "banana",
      "cherryThisStringIsLongerThanThirtyTwoBytes"
  };

  private static final int[] SINGLE_VALUE_ROWS = new int[]{2, 1, 0, 3};

  private static final IndexedInts[] MULTI_VALUE_ROWS = new IndexedInts[]{
      new ArrayBasedIndexedInts(new int[]{2, 1}),
      new ArrayBasedIndexedInts(new int[]{2}),
      new ArrayBasedIndexedInts(new int[]{0}),
      new ArrayBasedIndexedInts()
  };

  private ByteBuffer buf;
  @Mock
  private SingleValueDimensionVectorSelector singleValueSelector;
  @Mock
  private MultiValueDimensionVectorSelector multiValueSelector;

  private StringMinVectorAggregator singleValueTarget;
  private StringMinVectorAggregator multiValueTarget;
  private StringMinVectorAggregator customMultiValueTarget;

  @BeforeEach
  public void setUp()
  {
    Mockito.doReturn(true).when(singleValueSelector).nameLookupPossibleInAdvance();
    Mockito.doReturn(DICTIONARY.length).when(singleValueSelector).getValueCardinality();
    Mockito.doReturn(SINGLE_VALUE_ROWS).when(singleValueSelector).getRowVector();
    Mockito.doAnswer(invocation -> {
      int index = invocation.getArgument(0);
      return index >= DICTIONARY.length ? null : DICTIONARY[index];
    }).when(singleValueSelector).lookupName(anyInt());

    Mockito.doReturn(true).when(multiValueSelector).nameLookupPossibleInAdvance();
    Mockito.doReturn(DICTIONARY.length).when(multiValueSelector).getValueCardinality();
    Mockito.doReturn(MULTI_VALUE_ROWS).when(multiValueSelector).getRowVector();
    Mockito.doAnswer(invocation -> {
      int index = invocation.getArgument(0);
      return index >= DICTIONARY.length ? null : DICTIONARY[index];
    }).when(multiValueSelector).lookupName(anyInt());

    buf = ByteBuffer.allocate(BUFFER_SIZE);
    singleValueTarget = new StringMinVectorAggregator(singleValueSelector, null, MAX_STRING_BYTES, true);
    multiValueTarget = new StringMinVectorAggregator(null, multiValueSelector, MAX_STRING_BYTES, true);
    customMultiValueTarget = new StringMinVectorAggregator(null, multiValueSelector, MAX_STRING_BYTES, false);

    singleValueTarget.init(buf, POSITION);
  }

  @Test
  public void testInitSingleValueTarget()
  {
    singleValueTarget.init(buf, POSITION);
    Assertions.assertEquals(NOT_FOUND_FLAG_VALUE, buf.getInt(POSITION));
    Assertions.assertNull(singleValueTarget.get(buf, POSITION));
  }

  @Test
  public void testAggregateSingleValueFindsMinimum()
  {
    // rows: 2 ("banana"), 1 ("apple")
    singleValueTarget.aggregate(buf, POSITION, 0, 2);
    Assertions.assertEquals("apple", singleValueTarget.get(buf, POSITION));
  }

  @Test
  public void testAggregateSingleValueIgnoresNull()
  {
    // row 2 is null
    singleValueTarget.aggregate(buf, POSITION, 2, 3);
    Assertions.assertNull(singleValueTarget.get(buf, POSITION));

    // then aggregate row 0 ("banana")
    singleValueTarget.aggregate(buf, POSITION, 0, 1);
    Assertions.assertEquals("banana", singleValueTarget.get(buf, POSITION));
  }

  @Test
  public void testAggregateValueLongerThanLimitTruncates()
  {
    // row 3 is "cherryThisStringIsLongerThanThirtyTwoBytes"
    singleValueTarget.aggregate(buf, POSITION, 3, 4);
    Assertions.assertEquals("cherryThisStringIsLongerThanThir", singleValueTarget.get(buf, POSITION));
  }

  @Test
  public void testAggregateMultiValueMultipleValuesTrue()
  {
    multiValueTarget.init(buf, POSITION);
    // row 0: [2, 1] -> "banana", "apple"
    multiValueTarget.aggregate(buf, POSITION, 0, 1);
    Assertions.assertEquals("apple", multiValueTarget.get(buf, POSITION));
  }

  @Test
  public void testAggregateMultiValueMultipleValuesFalse()
  {
    customMultiValueTarget.init(buf, POSITION);
    // row 0: [2, 1] -> first is 2 ("banana")
    customMultiValueTarget.aggregate(buf, POSITION, 0, 1);
    Assertions.assertEquals("banana", customMultiValueTarget.get(buf, POSITION));
  }

  @Test
  public void testAggregateBatchSingleValue()
  {
    int[] positions = new int[]{0, 16, 32};
    int positionOffset = 4;
    for (int pos : positions) {
      singleValueTarget.init(buf, pos + positionOffset);
    }
    // Aggregate rows 0 ("banana"), 1 ("apple"), 3 ("cherry...")
    singleValueTarget.aggregate(buf, 3, positions, null, positionOffset);

    Assertions.assertEquals("banana", singleValueTarget.get(buf, positions[0] + positionOffset));
    Assertions.assertEquals("apple", singleValueTarget.get(buf, positions[1] + positionOffset));
    Assertions.assertNull(singleValueTarget.get(buf, positions[2] + positionOffset)); // row 2 was null!
  }
}

