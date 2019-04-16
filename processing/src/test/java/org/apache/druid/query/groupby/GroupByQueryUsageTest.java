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

package org.apache.druid.query.groupby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.UsageTestUtils;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.column.ValueType;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class GroupByQueryUsageTest
{
  private static final Closer resourceCloser = Closer.create();

  private final QueryRunner<Row> runner;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = Lists.newArrayList();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
        continue;
      }
      final Pair<GroupByQueryRunnerFactory, Closer> factoryAndCloser = GroupByQueryRunnerTest.makeQueryRunnerFactory(config);
      final GroupByQueryRunnerFactory factory = factoryAndCloser.lhs;
      resourceCloser.register(factoryAndCloser.rhs);
      for (QueryRunner<Row> runner : QueryRunnerTestHelper.makeQueryRunners(factory)) {
        constructors.add(new Object[]{factory, runner});
      }
    }

    return constructors;
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    resourceCloser.close();
  }

  public GroupByQueryUsageTest(
      GroupByQueryRunnerFactory factory,
      QueryRunner runner
  )
  {
    this.runner = factory.mergeRunners(MoreExecutors.sameThreadExecutor(), ImmutableList.of(runner));
  }

  private GroupByQuery.Builder newTestQuery()
  {
    return GroupByQuery.builder()
                       .setDataSource(QueryRunnerTestHelper.dataSource)
                       .setGranularity(QueryRunnerTestHelper.allGran)
                       .setDimFilter(UsageTestUtils.FILTER)
                       .setAggregatorSpecs(UsageTestUtils.AGGREGATOR_FACTORY)
                       .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval);
  }

  @Test
  public void testGroupByQueryWithSingleDimension()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("quality", "quality", ValueType.FLOAT))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    int nonNullColumns = 1 /* Dimensions */ +
                         1 /* AGGREGATOR_FACTORY */ +
                         1 /* FILTER */ +
                         2 /* EXPR_COLUMN */;
    UsageTestUtils.verify(runner, query, UsageTestUtils.numTestRows * nonNullColumns);
  }

  @Test
  public void testGroupByQueryWithMultiDimension()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("quality", "quality"),
                       new DefaultDimensionSpec("placement", "placement"))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    int nonNullColumns = 2 /* Dimensions */ +
                         1 /* AGGREGATOR_FACTORY */ +
                         1 /* FILTER */ +
                         2 /* EXPR_COLUMN */;
    UsageTestUtils.verify(runner, query, UsageTestUtils.numTestRows * nonNullColumns);
  }

  @Test
  public void testGroupByQueryWithEmptyColumn()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec(UsageTestUtils.NULL_COLUMN, UsageTestUtils.NULL_COLUMN))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    int nonNullColumns = 1 /* AGGREGATOR_FACTORY */ +
                         1 /* FILTER */ +
                         2 /* EXPR_COLUMN */;
    UsageTestUtils.verify(runner, query, UsageTestUtils.numTestRows * nonNullColumns);
  }

  @Test
  public void testGroupByQueryWithNullColumn()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("foo", "foo"))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    int nonNullColumns = 1 /* AGGREGATOR_FACTORY */ +
                         1 /* FILTER */ +
                         2 /* EXPR_COLUMN */;
    UsageTestUtils.verify(runner, query, UsageTestUtils.numTestRows * nonNullColumns);
  }

  @Test
  public void testGroupByQueryWithConstantVirtualColumn()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN,
                           UsageTestUtils.CONSTANT_COLUMN)
        .build();

    int nonNullColumns = 1 /* Dimensions */ +
                         1 /* AGGREGATOR_FACTORY */ +
                         1 /* FILTER */ +
                         2 /* EXPR_COLUMN */;
    UsageTestUtils.verify(runner, query, UsageTestUtils.numTestRows * nonNullColumns);
  }
}
