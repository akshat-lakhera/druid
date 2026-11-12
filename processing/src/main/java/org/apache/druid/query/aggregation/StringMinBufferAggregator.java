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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class StringMinBufferAggregator implements BufferAggregator
{
  @VisibleForTesting
  static final int NOT_FOUND_FLAG_VALUE = -1;
  @VisibleForTesting
  static final int FOUND_VALUE_OFFSET = Integer.BYTES;

  private final BaseObjectColumnValueSelector<?> valueSelector;
  private final int maxStringBytes;
  private final boolean aggregateMultipleValues;
  private final byte[] scratch;
  private final ByteBuffer scratchBuffer;

  public StringMinBufferAggregator(
      BaseObjectColumnValueSelector<?> valueSelector,
      int maxStringBytes,
      boolean aggregateMultipleValues
  )
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.aggregateMultipleValues = aggregateMultipleValues;
    this.scratch = new byte[maxStringBytes];
    this.scratchBuffer = ByteBuffer.wrap(scratch);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putInt(position, NOT_FOUND_FLAG_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
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
          aggregateOne(buf, position, elem);
        }
      } else {
        aggregateOne(buf, position, list.get(0));
      }
    } else {
      aggregateOne(buf, position, object);
    }
  }

  private void aggregateOne(ByteBuffer buf, int position, @Nullable Object elem)
  {
    if (elem == null) {
      return;
    }
    final String val = DimensionHandlerUtils.convertObjectToString(elem);
    if (val == null) {
      return;
    }
    final int currentLen = buf.getInt(position);
    if (currentLen == NOT_FOUND_FLAG_VALUE) {
      ByteBuffer mutationBuffer = buf.duplicate();
      mutationBuffer.position(position + FOUND_VALUE_OFFSET);
      mutationBuffer.limit(position + FOUND_VALUE_OFFSET + maxStringBytes);
      final int len = StringUtils.toUtf8WithLimit(val, mutationBuffer);
      buf.putInt(position, len);
    } else {
      scratchBuffer.clear();
      final int valLen = StringUtils.toUtf8WithLimit(val, scratchBuffer);
      final int cmp = compareUtf8(scratch, 0, valLen, buf, position + FOUND_VALUE_OFFSET, currentLen);
      if (cmp < 0) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position + FOUND_VALUE_OFFSET);
        mutationBuffer.put(scratch, 0, valLen);
        buf.putInt(position, valLen);
      }
    }
  }

  private static int compareUtf8(
      final byte[] a,
      final int aOffset,
      final int aLen,
      final ByteBuffer bBuf,
      final int bOffset,
      final int bLen
  )
  {
    final int commonLength = Math.min(aLen, bLen);
    for (int i = 0; i < commonLength; i++) {
      final int byte1 = a[aOffset + i] & 0xFF;
      final int byte2 = bBuf.get(bOffset + i) & 0xFF;
      final int cmp = byte1 - byte2;
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(aLen, bLen);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final int stringSizeBytes = buf.getInt(position);
    if (stringSizeBytes >= 0) {
      final byte[] valueBytes = new byte[stringSizeBytes];
      ByteBuffer copyBuffer = buf.duplicate();
      copyBuffer.position(position + FOUND_VALUE_OFFSET);
      copyBuffer.get(valueBytes, 0, stringSizeBytes);
      return StringUtils.fromUtf8(valueBytes);
    } else {
      return null;
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringMinBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringMinBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringMinBufferAggregator does not support getDouble()");
  }

  @Override
  public boolean isNull(ByteBuffer buf, int position)
  {
    return buf.getInt(position) == NOT_FOUND_FLAG_VALUE;
  }

  @Override
  public void close()
  {
    // no-op
  }
}

