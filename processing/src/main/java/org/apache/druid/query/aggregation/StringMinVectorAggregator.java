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
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringMinVectorAggregator implements VectorAggregator
{
  @VisibleForTesting
  static final int NOT_FOUND_FLAG_VALUE = -1;

  @Nullable
  private final SingleValueDimensionVectorSelector singleValueSelector;
  @Nullable
  private final MultiValueDimensionVectorSelector multiValueSelector;
  private final int maxStringBytes;
  private final boolean aggregateMultipleValues;
  private final int nullId;

  public StringMinVectorAggregator(
      @Nullable final SingleValueDimensionVectorSelector singleValueSelector,
      @Nullable final MultiValueDimensionVectorSelector multiValueSelector,
      final int maxStringBytes,
      final boolean aggregateMultipleValues
  )
  {
    Preconditions.checkState(
        singleValueSelector != null || multiValueSelector != null,
        "At least one selector must be non null"
    );
    Preconditions.checkState(
        singleValueSelector == null || multiValueSelector == null,
        "Only one selector must be non null"
    );
    this.singleValueSelector = singleValueSelector;
    this.multiValueSelector = multiValueSelector;
    this.maxStringBytes = maxStringBytes;
    this.aggregateMultipleValues = aggregateMultipleValues;

    final DimensionDictionarySelector dictSelector = singleValueSelector != null
                                                     ? singleValueSelector
                                                     : multiValueSelector;
    if (dictSelector.nameLookupPossibleInAdvance() && dictSelector.getValueCardinality() > 0) {
      this.nullId = dictSelector.lookupName(0) == null ? 0 : -1;
    } else {
      this.nullId = -1;
    }
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putInt(position, NOT_FOUND_FLAG_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    int minId = buf.getInt(position);
    if (singleValueSelector != null) {
      final int[] rows = singleValueSelector.getRowVector();
      final int limit = Math.min(endRow, rows.length);
      for (int i = startRow; i < limit; i++) {
        final int id = rows[i];
        if (id != nullId && id >= 0) {
          if (minId == NOT_FOUND_FLAG_VALUE || id < minId) {
            minId = id;
          }
        }
      }
    } else if (multiValueSelector != null) {
      final IndexedInts[] rows = multiValueSelector.getRowVector();
      final int limit = Math.min(endRow, rows.length);
      for (int i = startRow; i < limit; i++) {
        final IndexedInts row = rows[i];
        if (row == null || row.size() == 0) {
          continue;
        }
        if (aggregateMultipleValues) {
          final int sz = row.size();
          for (int j = 0; j < sz; j++) {
            final int id = row.get(j);
            if (id != nullId && id >= 0) {
              if (minId == NOT_FOUND_FLAG_VALUE || id < minId) {
                minId = id;
              }
            }
          }
        } else {
          final int id = row.get(0);
          if (id != nullId && id >= 0) {
            if (minId == NOT_FOUND_FLAG_VALUE || id < minId) {
              minId = id;
            }
          }
        }
      }
    }
    buf.putInt(position, minId);
  }

  @Override
  public void aggregate(
      ByteBuffer buf,
      int numRows,
      int[] positions,
      @Nullable int[] rows,
      int positionOffset
  )
  {
    if (singleValueSelector != null) {
      final int[] rowVector = singleValueSelector.getRowVector();
      for (int i = 0; i < numRows; i++) {
        final int row = rows == null ? i : rows[i];
        final int id = rowVector[row];
        if (id != nullId && id >= 0) {
          final int position = positions[i] + positionOffset;
          final int currentMin = buf.getInt(position);
          if (currentMin == NOT_FOUND_FLAG_VALUE || id < currentMin) {
            buf.putInt(position, id);
          }
        }
      }
    } else if (multiValueSelector != null) {
      final IndexedInts[] rowVector = multiValueSelector.getRowVector();
      for (int i = 0; i < numRows; i++) {
        final int row = rows == null ? i : rows[i];
        final IndexedInts rowInts = rowVector[row];
        if (rowInts == null || rowInts.size() == 0) {
          continue;
        }
        final int position = positions[i] + positionOffset;
        int currentMin = buf.getInt(position);
        if (aggregateMultipleValues) {
          final int sz = rowInts.size();
          for (int j = 0; j < sz; j++) {
            final int id = rowInts.get(j);
            if (id != nullId && id >= 0) {
              if (currentMin == NOT_FOUND_FLAG_VALUE || id < currentMin) {
                currentMin = id;
              }
            }
          }
        } else {
          final int id = rowInts.get(0);
          if (id != nullId && id >= 0) {
            if (currentMin == NOT_FOUND_FLAG_VALUE || id < currentMin) {
              currentMin = id;
            }
          }
        }
        buf.putInt(position, currentMin);
      }
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final int minId = buf.getInt(position);
    if (minId < 0) {
      return null;
    }
    final DimensionDictionarySelector selector = singleValueSelector != null
                                                 ? singleValueSelector
                                                 : multiValueSelector;
    final String val = selector.lookupName(minId);
    return StringUtils.chop(val, maxStringBytes);
  }

  @Override
  public void close()
  {
    // no-op
  }
}

