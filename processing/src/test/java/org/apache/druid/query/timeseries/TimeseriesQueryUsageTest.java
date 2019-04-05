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

package org.apache.druid.query.timeseries;

import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.UsageTestUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class TimeseriesQueryUsageTest
{
  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        // runners
        QueryRunnerTestHelper.makeQueryRunners(
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        ),
        // descending?
        Arrays.asList(false, true),
        Arrays.asList(QueryRunnerTestHelper.commonDoubleAggregators, QueryRunnerTestHelper.commonFloatAggregators)
    );
  }

  protected final QueryRunner runner;
  protected final boolean descending;
  private final List<AggregatorFactory> aggregatorFactoryList;

  public TimeseriesQueryUsageTest(
      QueryRunner runner, boolean descending,
      List<AggregatorFactory> aggregatorFactoryList
  )
  {
    this.runner = runner;
    this.descending = descending;
    this.aggregatorFactoryList = aggregatorFactoryList;
  }

  private Druids.TimeseriesQueryBuilder newTestRunner()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(QueryRunnerTestHelper.dataSource)
                 .granularity(QueryRunnerTestHelper.allGran)
                 .intervals(QueryRunnerTestHelper.fullOnInterval)
                 .aggregators(aggregatorFactoryList)
                 .filters(UsageTestUtils.FILTER)
                 .descending(descending);
  }

  private int getExpectedValue()
  {
    if (aggregatorFactoryList == QueryRunnerTestHelper.commonFloatAggregators) {
      return 4185;
    }
    return 3348;
  }

  @Test
  public void testTimeseriesQuery()
  {
    TimeseriesQuery query = newTestRunner()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, getExpectedValue());
  }

  @Test
  public void testTimeseriesQueryWithEmptyColumn()
  {
    TimeseriesQuery query = newTestRunner()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.EMPTY_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, getExpectedValue());
  }

  @Test
  public void testTimeseriesQueryWithNullColumn()
  {
    TimeseriesQuery query = newTestRunner()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.FOO_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, getExpectedValue());
  }

  @Test
  public void testTimeseriesQueryWithConstantVirtualColumn()
  {
    TimeseriesQuery query = newTestRunner()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.CONSTANT_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, getExpectedValue());
  }
}
