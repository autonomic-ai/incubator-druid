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
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.select.PagingSpec;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class UsageUtilsTest
{
  private static Random random;
  private static List<String> columns;
  private Set<String> requiredColumns;

  @BeforeClass
  public static void init()
  {
    random = new Random(1);
    columns = new ArrayList<>();
    for (char column = 'a'; column <= 'z'; column++) {
      columns.add(String.valueOf(column));
    }
  }

  private static List<String> randomSublist(List<String> sourceList)
  {
    List<String> pickedList = new ArrayList<>();
    int size = 3;
    for (int i = 0; i < sourceList.size(); i++) {
      if (sourceList.size() - i == size - pickedList.size()) {
        for (int j = i; j < sourceList.size(); j++) {
          pickedList.add(sourceList.get(j));
        }
        break;
      }
      if (random.nextBoolean()) {
        pickedList.add(sourceList.get(i));
      }
    }

    return pickedList;
  }

  private static String randomExpression(List<String> columns)
  {
    char[] operations = {'+', '-', '*', '/'};
    StringBuilder expression = new StringBuilder();
    for (String column : columns) {
      expression.append(column).append(operations[random.nextInt(operations.length)]);
    }
    expression.append("3");
    return expression.toString();
  }

  private static DimFilter randomFilter(Set<String> requiredColumns)
  {
    String selectedColumn = columns.get(random.nextInt(columns.size()));
    requiredColumns.add(selectedColumn);
    DimFilter selectDimFilter = new SelectorDimFilter(selectedColumn, "", null);

    String columnBound = columns.get(random.nextInt(columns.size()));
    requiredColumns.add(columnBound);
    DimFilter boundDimFilter = new BoundDimFilter(columnBound, "", "", false,
                                                   false, false, null, null);

    List<String> columnsInvolved = randomSublist(columns);
    requiredColumns.addAll(columnsInvolved);
    DimFilter expressionDimFilter = new ExpressionDimFilter(randomExpression(columnsInvolved), null);
    return new OrDimFilter(new AndDimFilter(selectDimFilter, boundDimFilter), expressionDimFilter);
  }

  private static VirtualColumn randomVirtualColumn(Set<String> requiredColumns)
  {
    List<String> columnsInvolved = randomSublist(columns);
    requiredColumns.addAll(columnsInvolved);
    return new ExpressionVirtualColumn("v0", randomExpression(columnsInvolved),
                                       null,
                                       null);
  }


  private static List<DimensionSpec> randomDimensionSpecs(Set<String> requiredColumns)
  {
    List<String> columnsInvolved = randomSublist(columns);
    requiredColumns.addAll(columnsInvolved);
    List<DimensionSpec> dimensionSpecs = new ArrayList<>();
    for (String column : columnsInvolved) {
      dimensionSpecs.add(new DefaultDimensionSpec(column, "d" + column, null));
    }
    return dimensionSpecs;
  }

  private static List<AggregatorFactory> randomAggregators(Set<String> requiredColumns)
  {
    List<AggregatorFactory> aggregatorFactories = new ArrayList<>();
    List<String> columnsInvolved = randomSublist(columns);
    requiredColumns.addAll(columnsInvolved);
    for (String column : columnsInvolved) {
      aggregatorFactories.add(
          new StringFirstAggregatorFactory("vs" + column, column, null)
      );
      aggregatorFactories.add(
          new DoubleSumAggregatorFactory("vd" + column, column)
      );
    }
    return aggregatorFactories;
  }

  private static void verify(Set<String> expectedColumns, Set<String> columns)
  {
    Assert.assertEquals(expectedColumns.size(), columns.size());
    for (String column : expectedColumns) {
      Assert.assertTrue(columns.contains(column));
    }
  }

  @Before
  public void construct()
  {
    requiredColumns = new HashSet<>();
  }

  @Test
  public void testRequiredColumnsOfScan()
  {
    List<String> columnsInvolved = randomSublist(columns);
    this.requiredColumns.addAll(columnsInvolved);
    ScanQuery scanQuery = ScanQuery.newScanQueryBuilder()
                                   .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                                   .intervals(QueryRunnerTestHelper.fullOnInterval)
                                   .columns(columnsInvolved)
                                   .virtualColumns(randomVirtualColumn(this.requiredColumns))
                                   .filters(randomFilter(this.requiredColumns))
                                   .build();

    Set<String> requiredColumns = UsageUtils.getRequiredColumns(
        null,
        scanQuery.getVirtualColumns(),
        scanQuery.getFilter(),
        null,
        scanQuery.getColumns()
    );
    verify(this.requiredColumns, requiredColumns);
  }

  @Test
  public void testRequiredColumnsOfSelect()
  {
    List<String> columnsInvolved = randomSublist(columns);
    this.requiredColumns.addAll(columnsInvolved);
    SelectQuery selectQuery = Druids.newSelectQueryBuilder()
                                    .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .pagingSpec(PagingSpec.newSpec(3))
                                    .dimensionSpecs(randomDimensionSpecs(this.requiredColumns))
                                    .metrics(columnsInvolved)
                                    .filters(randomFilter(this.requiredColumns))
                                    .virtualColumns(randomVirtualColumn(this.requiredColumns))
                                    .build();
    Set<String> requiredColumns = UsageUtils.getRequiredColumns(
        selectQuery.getDimensions(),
        selectQuery.getVirtualColumns(),
        selectQuery.getFilter(),
        null,
        selectQuery.getMetrics()
    );
    verify(this.requiredColumns, requiredColumns);
  }

  @Test
  public void testRequiredColumnsOfTimeseries()
  {
    TimeseriesQuery timeseriesQuery = Druids.newTimeseriesQueryBuilder()
                                            .dataSource(QueryRunnerTestHelper.dataSource)
                                            .intervals(QueryRunnerTestHelper.firstToThird)
                                            .aggregators(randomAggregators(this.requiredColumns))
                                            .virtualColumns(randomVirtualColumn(this.requiredColumns))
                                            .filters(randomFilter(this.requiredColumns))
                                            .build();
    Set<String> requiredColumns = UsageUtils.getRequiredColumns(
        null,
        timeseriesQuery.getVirtualColumns(),
        timeseriesQuery.getFilter(),
        timeseriesQuery.getAggregatorSpecs(),
        null
    );
    this.requiredColumns.add("__time");
    verify(this.requiredColumns, requiredColumns);
  }

  @Test
  public void testRequiredColumnsOfGroupBy()
  {
    GroupByQuery groupByQuery = GroupByQuery.builder()
                                            .setDataSource(QueryRunnerTestHelper.dataSource)
                                            .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                            .setGranularity(QueryRunnerTestHelper.allGran)
                                            .setDimensions(randomDimensionSpecs(this.requiredColumns))
                                            .setAggregatorSpecs(randomAggregators(this.requiredColumns))
                                            .setVirtualColumns(randomVirtualColumn(this.requiredColumns))
                                            .setDimFilter(randomFilter(this.requiredColumns))
                                            .build();
    Set<String> requiredColumns = UsageUtils.getRequiredColumns(
        groupByQuery.getDimensions(),
        groupByQuery.getVirtualColumns(),
        groupByQuery.getDimFilter(),
        groupByQuery.getAggregatorSpecs(),
        null
    );
    this.requiredColumns.add("__time");
    verify(this.requiredColumns, requiredColumns);
  }
}
