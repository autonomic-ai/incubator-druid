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

package org.apache.druid.query.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.UsageUtils;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class SearchQuery extends BaseQuery<Result<SearchResultValue>>
{
  private static final SearchSortSpec DEFAULT_SORT_SPEC = new SearchSortSpec(StringComparators.LEXICOGRAPHIC);

  private final DimFilter dimFilter;
  private final SearchSortSpec sortSpec;
  private final List<DimensionSpec> dimensions;
  private final SearchQuerySpec querySpec;
  private final int limit;

  private final UsageUtils.UsageCollector usageCollector;

  @JsonCreator
  public SearchQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("limit") int limit,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("searchDimensions") List<DimensionSpec> dimensions,
      @JsonProperty("query") SearchQuerySpec querySpec,
      @JsonProperty("sort") SearchSortSpec sortSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this(
        dataSource,
        dimFilter,
        granularity,
        limit,
        querySegmentSpec,
        dimensions,
        querySpec,
        sortSpec,
        context,
        null
    );
  }

  public SearchQuery(
      final DataSource dataSource,
      final DimFilter dimFilter,
      final Granularity granularity,
      final int limit,
      final QuerySegmentSpec querySegmentSpec,
      final List<DimensionSpec> dimensions,
      final SearchQuerySpec querySpec,
      final SearchSortSpec sortSpec,
      final Map<String, Object> context,
      final UsageUtils.UsageCollector usageCollector
  )
  {
    super(dataSource, querySegmentSpec, false, context, Granularities.nullToAll(granularity));
    Preconditions.checkNotNull(querySegmentSpec, "Must specify an interval");

    this.dimFilter = dimFilter;
    this.sortSpec = sortSpec == null ? DEFAULT_SORT_SPEC : sortSpec;
    this.limit = (limit == 0) ? 1000 : limit;
    this.dimensions = dimensions;
    this.querySpec = querySpec == null ? new AllSearchQuerySpec() : querySpec;

    if (usageCollector == null) {
      this.usageCollector = new UsageUtils.UsageCollector(
          new AtomicLong(0),
          dimensions,
          null,
          dimFilter,
          null,
          null
      );
    } else {
      this.usageCollector = usageCollector;
    }
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return Query.SEARCH;
  }

  @Override
  public SearchQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return Druids.SearchQueryBuilder.copy(this).intervals(spec).build();
  }

  @Override
  public Query<Result<SearchResultValue>> withDataSource(DataSource dataSource)
  {
    return Druids.SearchQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  @Override
  public SearchQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    Map<String, Object> newContext = computeOverriddenContext(getContext(), contextOverrides);
    return Druids.SearchQueryBuilder.copy(this).context(newContext).build();
  }

  public SearchQuery withDimFilter(DimFilter dimFilter)
  {
    return Druids.SearchQueryBuilder.copy(this).filters(dimFilter).build();
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty("searchDimensions")
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("query")
  public SearchQuerySpec getQuery()
  {
    return querySpec;
  }

  @JsonProperty("sort")
  public SearchSortSpec getSort()
  {
    return sortSpec;
  }

  public SearchQuery withLimit(int newLimit)
  {
    return Druids.SearchQueryBuilder.copy(this).limit(newLimit).build();
  }

  @Override
  public UsageUtils.UsageCollector getUsageCollector()
  {
    return usageCollector;
  }

  @Override
  public String toString()
  {
    return "SearchQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", dimFilter=" + dimFilter +
           ", granularity='" + getGranularity() + '\'' +
           ", dimensions=" + dimensions +
           ", querySpec=" + querySpec +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", limit=" + limit +
           '}';
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
    if (!super.equals(o)) {
      return false;
    }

    SearchQuery that = (SearchQuery) o;

    if (limit != that.limit) {
      return false;
    }
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) {
      return false;
    }
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) {
      return false;
    }
    if (querySpec != null ? !querySpec.equals(that.querySpec) : that.querySpec != null) {
      return false;
    }
    if (sortSpec != null ? !sortSpec.equals(that.sortSpec) : that.sortSpec != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (sortSpec != null ? sortSpec.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (querySpec != null ? querySpec.hashCode() : 0);
    result = 31 * result + limit;
    return result;
  }
}
