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

package org.apache.druid.query.window;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;

/**
 * An abstract class for a query with GroupBy over a Window subquery.
 * The actual implementation is defined in the extension.
 */
public abstract class GroupByOverWindowBaseQuery extends GroupByQuery
{
  public GroupByOverWindowBaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      VirtualColumns virtualColumns,
      DimFilter dimFilter,
      Granularity granularity,
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggregatorSpecs,
      List<PostAggregator> postAggregatorSpecs,
      HavingSpec havingSpec,
      LimitSpec limitSpec,
      List<List<String>> subtotalsSpec,
      Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        virtualColumns,
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        subtotalsSpec,
        context
    );
  }
}
