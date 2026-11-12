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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.any.NilVectorAggregator;
import org.apache.druid.query.aggregation.firstlast.first.StringFirstAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Comparator;

public class StringMinAggregatorFactory extends SimpleStringAggregatorFactory
{
  public static final Comparator<String> COMPARATOR = Comparator.nullsFirst(StringUtils::compareUnicode);

  @JsonCreator
  public StringMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") @Nullable final String fieldName,
      @JsonProperty("expression") @Nullable String expression,
      @JsonProperty("maxStringBytes") @Nullable Integer maxStringBytes,
      @JsonProperty("aggregateMultipleValues") @Nullable final Boolean aggregateMultipleValues,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(name, fieldName, expression, maxStringBytes, aggregateMultipleValues, macroTable);
  }

  public StringMinAggregatorFactory(String name, String fieldName)
  {
    this(
        name,
        fieldName,
        null,
        StringFirstAggregatorFactory.DEFAULT_MAX_STRING_SIZE,
        true,
        ExprMacroTable.nil()
    );
  }

  public StringMinAggregatorFactory(String name, String fieldName, Integer maxStringBytes)
  {
    this(
        name,
        fieldName,
        null,
        maxStringBytes,
        true,
        ExprMacroTable.nil()
    );
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new StringMinAggregator(
        makeColumnValueSelector(metricFactory),
        maxStringBytes,
        aggregateMultipleValues
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new StringMinBufferAggregator(
        makeColumnValueSelector(metricFactory),
        maxStringBytes,
        aggregateMultipleValues
    );
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    if (capabilities == null) {
      return NilVectorAggregator.createNilVectorAggregator();
    }
    if (capabilities.hasMultipleValues().isMaybeTrue()) {
      return new StringMinVectorAggregator(
          null,
          selectorFactory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(fieldName)),
          maxStringBytes,
          aggregateMultipleValues
      );
    } else {
      return new StringMinVectorAggregator(
          selectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(fieldName)),
          null,
          maxStringBytes,
          aggregateMultipleValues
      );
    }
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (lhs == null) {
      return rhs;
    }
    if (rhs == null) {
      return lhs;
    }
    final String s1 = DimensionHandlerUtils.convertObjectToString(lhs);
    final String s2 = DimensionHandlerUtils.convertObjectToString(rhs);
    if (s1 == null) {
      return s2;
    }
    if (s2 == null) {
      return s1;
    }
    final String min = StringUtils.compareUnicode(s1, s2) <= 0 ? s1 : s2;
    return StringUtils.chop(min, maxStringBytes);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringMinAggregatorFactory(
        name,
        name,
        null,
        maxStringBytes,
        aggregateMultipleValues,
        macroTable
    );
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new StringMinAggregatorFactory(
        newName,
        fieldName,
        expression,
        maxStringBytes,
        aggregateMultipleValues,
        macroTable
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    if (fieldName != null) {
      return new CacheKeyBuilder(AggregatorUtil.STRING_MIN_CACHE_TYPE_ID)
          .appendString(fieldName)
          .appendInt(maxStringBytes)
          .appendBoolean(aggregateMultipleValues)
          .build();
    } else {
      return new CacheKeyBuilder(AggregatorUtil.STRING_MIN_CACHE_TYPE_ID)
          .appendString(fieldExpression.get().stringify())
          .appendInt(maxStringBytes)
          .appendBoolean(aggregateMultipleValues)
          .build();
    }
  }
}

