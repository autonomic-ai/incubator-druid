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

package org.apache.druid.query;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class UsageUtils
{
  public static Set<String> getRequiredColumns(
      @Nullable List<DimensionSpec> dimensionSpecs,
      @Nullable VirtualColumns virtualColumns,
      @Nullable DimFilter dimFilter,
      @Nullable List<AggregatorFactory> aggregatorFactories,
      @Nullable List<String> columns
  )
  {
    Set<String> requiredColumns = new HashSet<>();

    if (aggregatorFactories != null) {
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        requiredColumns.addAll(aggregatorFactory.requiredFields());
      }
    }

    if (dimFilter != null) {
      requiredColumns.addAll(dimFilter.getRequiredColumns());
    }

    if (dimensionSpecs != null) {
      for (DimensionSpec dimensionSpec : dimensionSpecs) {
        requiredColumns.add(dimensionSpec.getDimension());
      }
    }

    if (columns != null) {
      requiredColumns.addAll(columns);
    }

    if (virtualColumns != null) {
      for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
        requiredColumns.addAll(virtualColumn.requiredColumns());
        requiredColumns.remove(virtualColumn.getOutputName());
      }
    }

    return requiredColumns;
  }

  private static void incrementAuSignals(AtomicLong numAuSignals, List<ColumnValueSelector> columnValueSelectors)
  {
    if (numAuSignals == null) {
      return;
    }
    int columnInvolved = 0;
    for (ColumnValueSelector columnValueSelector : columnValueSelectors) {
      Object value = columnValueSelector.getObject();
      if (isEmpty(value)) {
        continue;
      }
      columnInvolved++;
    }
    numAuSignals.addAndGet(columnInvolved);
  }

  private static boolean isEmpty(Object value)
  {
    if (value == null || "".equals(value)) {
      return true;
    }

    if (value instanceof Number) {
      return (((Number) value).doubleValue()) == 0;
    }
    return false;
  }

  public static class UsageCollector
  {
    AtomicLong numAuSignals;
    Set<String> requiredColumns;
    ConcurrentMap<Cursor, List<ColumnValueSelector>> selectorsMap;

    public UsageCollector(
        AtomicLong numAuSignals,
        @Nullable List<DimensionSpec> dimensionSpecs,
        @Nullable VirtualColumns virtualColumns,
        @Nullable DimFilter dimFilter,
        @Nullable List<AggregatorFactory> aggregatorFactories,
        @Nullable List<String> columns
    )
    {
      this.numAuSignals = numAuSignals;
      this.requiredColumns = getRequiredColumns(
          dimensionSpecs,
          virtualColumns,
          dimFilter,
          aggregatorFactories,
          columns
      );

      this.selectorsMap = new ConcurrentHashMap<>();
    }

    public void createSelectors(Cursor cursor)
    {
      List<ColumnValueSelector> selectors = new ArrayList<>();
      for (String requiredColumn : requiredColumns) {
        ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory()
                                                        .makeColumnValueSelector(requiredColumn);
        if (columnValueSelector instanceof NilColumnValueSelector) {
          continue;
        }
        selectors.add(columnValueSelector);
      }
      selectorsMap.put(cursor, selectors);
    }

    public void removeSelectors(Cursor cursor)
    {
      selectorsMap.remove(cursor);
    }

    public void collect(Cursor cursor)
    {
      UsageUtils.incrementAuSignals(numAuSignals, selectorsMap.get(cursor));
    }

    public AtomicLong getNumAuSignals()
    {
      return numAuSignals;
    }
  }
}
