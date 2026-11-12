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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.firstlast.first.StringFirstAggregatorFactory;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionSelectors;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Abstract factory for aggregators that produce string values (such as StringMin and StringMax).
 */
public abstract class SimpleStringAggregatorFactory extends AggregatorFactory
{
  protected final String name;
  @Nullable
  protected final String fieldName;
  @Nullable
  protected final String expression;
  protected final int maxStringBytes;
  protected final boolean aggregateMultipleValues;
  protected final ExprMacroTable macroTable;
  protected final Supplier<Expr> fieldExpression;

  public SimpleStringAggregatorFactory(
      final String name,
      @Nullable final String fieldName,
      @Nullable final String expression,
      @Nullable final Integer maxStringBytes,
      @Nullable final Boolean aggregateMultipleValues,
      final ExprMacroTable macroTable
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ expression == null,
        "Must have a valid, non-null fieldName or expression"
    );
    this.name = name;
    this.fieldName = fieldName;
    this.expression = expression;
    this.macroTable = macroTable;
    this.fieldExpression = Parser.lazyParse(expression, macroTable);
    this.maxStringBytes = maxStringBytes == null
                          ? StringFirstAggregatorFactory.DEFAULT_MAX_STRING_SIZE
                          : maxStringBytes;
    if (this.maxStringBytes < 0) {
      throw new IAE("maxStringBytes must be greater than or equal to 0");
    }
    this.aggregateMultipleValues = aggregateMultipleValues == null ? true : aggregateMultipleValues;
  }

  protected ColumnValueSelector<?> makeColumnValueSelector(ColumnSelectorFactory metricFactory)
  {
    if (fieldName != null) {
      return metricFactory.makeColumnValueSelector(fieldName);
    } else {
      return ExpressionSelectors.makeStringColumnValueSelector(metricFactory, fieldExpression.get());
    }
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    if (fieldName == null) {
      return false;
    }
    final ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
    return capabilities == null
           || (capabilities.is(ValueType.STRING) && capabilities.isDictionaryEncoded().isTrue());
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.STRING;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.STRING;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Integer.BYTES + maxStringBytes;
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldName != null
           ? Collections.singletonList(fieldName)
           : fieldExpression.get().analyzeInputs().getRequiredBindingsList();
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public int getMaxStringBytes()
  {
    return maxStringBytes;
  }

  @JsonProperty
  public boolean getAggregateMultipleValues()
  {
    return aggregateMultipleValues;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleStringAggregatorFactory that = (SimpleStringAggregatorFactory) o;
    return maxStringBytes == that.maxStringBytes &&
           aggregateMultipleValues == that.aggregateMultipleValues &&
           Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName) &&
           Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, expression, maxStringBytes, aggregateMultipleValues);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", expression='" + expression + '\'' +
           ", maxStringBytes=" + maxStringBytes +
           ", aggregateMultipleValues=" + aggregateMultipleValues +
           '}';
  }
}

