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

package org.apache.druid.sql.calcite.window;

import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.window.GroupByOverWindowBaseQuery;
import org.apache.druid.query.window.WindowBaseQuery;
import org.apache.druid.query.window.WindowQueryFactory;
import org.apache.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;

public class MockWindowQueryFactory implements WindowQueryFactory
{
  public class MockWindowQuery extends WindowBaseQuery
  {
    public MockWindowQuery(
        DataSource dataSource, QuerySegmentSpec querySegmentSpec, boolean descending, Map<String, Object> context
    )
    {
      super(dataSource, querySegmentSpec, descending, context);
    }

    @Override
    public DimFilter getFilter()
    {
      return null;
    }

    @Override
    public Query<Row> withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      return null;
    }

    @Override
    public Query<Row> withDataSource(DataSource dataSource)
    {
      return null;
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public boolean hasFilters()
    {
      return false;
    }

    @Override
    public Query<Row> withOverriddenContext(Map<String, Object> contextOverride)
    {
      return null;
    }

    @Override
    public Query<Row> optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
    {
      return null;
    }
  }

  public class MockGroupByOverWindowQuery extends GroupByOverWindowBaseQuery
  {
    public MockGroupByOverWindowQuery(
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



  @Override
  public WindowBaseQuery factorize(
      DataSource dataSource, QuerySegmentSpec querySegmentSpec,
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggregations,
      List<PostAggregator> postAggregators,
      List<DimensionSpec> partitions, DimFilter dimFilter,
      LimitSpec limitSpec, Map<String, Object> queryContext
  )
  {
    return new MockWindowQuery(dataSource, querySegmentSpec, true, queryContext);
  }

  @Override
  public GroupByOverWindowBaseQuery factorize(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      VirtualColumns virtualColumns,
      DimFilter dimFilter,
      Granularity granularity,
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggregatorSpecs,
      List<PostAggregator> postAggregatorSpecs,
      HavingSpec havingSpec, LimitSpec limitSpec,
      List<List<String>> subtotalsSpec,
      Map<String, Object> context
  )
  {
    return new MockGroupByOverWindowQuery(dataSource, querySegmentSpec, virtualColumns,
                                          dimFilter, granularity, dimensions, aggregatorSpecs,
                                          postAggregatorSpecs, havingSpec, limitSpec, subtotalsSpec,
                                          context
    );
  }
}
