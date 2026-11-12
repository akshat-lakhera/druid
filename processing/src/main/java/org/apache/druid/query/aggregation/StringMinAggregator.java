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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

import javax.annotation.Nullable;
import java.util.List;

public class StringMinAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector<?> valueSelector;
  private final int maxStringBytes;
  private final boolean aggregateMultipleValues;
  @Nullable
  private String min;

  public StringMinAggregator(
      BaseObjectColumnValueSelector<?> valueSelector,
      int maxStringBytes,
      boolean aggregateMultipleValues
  )
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.aggregateMultipleValues = aggregateMultipleValues;
    this.min = null;
  }

  @Override
  public void aggregate()
  {
    final Object object = valueSelector.getObject();
    if (object == null) {
      return;
    }
    if (object instanceof List) {
      final List<?> list = (List<?>) object;
      if (list.isEmpty()) {
        return;
      }
      if (aggregateMultipleValues) {
        for (Object elem : list) {
          updateMin(elem);
        }
      } else {
        updateMin(list.get(0));
      }
    } else {
      updateMin(object);
    }
  }

  private void updateMin(@Nullable final Object elem)
  {
    if (elem == null) {
      return;
    }
    final String val = DimensionHandlerUtils.convertObjectToString(elem);
    if (val == null) {
      return;
    }
    final String chopped = StringUtils.chop(val, maxStringBytes);
    if (min == null || StringUtils.compareUnicode(chopped, min) < 0) {
      min = chopped;
    }
  }

  @Nullable
  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("StringMinAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("StringMinAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("StringMinAggregator does not support getDouble()");
  }

  @Override
  public boolean isNull()
  {
    return min == null;
  }

  @Override
  public void close()
  {
    // no-op
  }
}

