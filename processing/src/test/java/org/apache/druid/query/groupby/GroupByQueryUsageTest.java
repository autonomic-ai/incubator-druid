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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class GroupByQueryUsageTest
{
  private static final Closer resourceCloser = Closer.create();

  private final QueryRunner<Row> runner;
  private final GroupByQueryConfig config;
  private final String runnerName;

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
        constructors.add(new Object[]{config, factory, runner});
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
      GroupByQueryConfig config,
      GroupByQueryRunnerFactory factory,
      QueryRunner runner
  )
  {
    this.runnerName = runner.toString();
    this.config = config;
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

  private static final Set<String> SPECIAL_CONFIG = new HashSet<>(Arrays.asList(
      "noRollupRtIndex",
      "mMappedTestIndex",
      "noRollupMMappedTestIndex",
      "mergedRealtimeIndex",
      "rtIndex"
  ));

  private int getExpectedValue()
  {
    if (config.toString().equals("v2SmallBuffer")
        && SPECIAL_CONFIG.contains(runnerName)) {
      return 6275;
    }
    return 4185;
  }

  @Test
  public void testGroupByQuerySingleDimension()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("quality", "quality", ValueType.FLOAT))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 4185);
  }

  @Test
  public void testGroupByQueryMultiDimension()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("quality", "quality"),
                       new DefaultDimensionSpec("index", "index"))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, getExpectedValue());
  }

  @Test
  public void testGroupByQueryWithEmptyColumn()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec(UsageTestUtils.NULL_COLUMN, UsageTestUtils.NULL_COLUMN))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 3348);
  }

  @Test
  public void testGroupByQueryWithNullColumn()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("foo", "foo"))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();
    UsageTestUtils.verify(runner, query, 3348);
  }

  @Test
  public void testGroupByQueryWithConstantVirtualColumn()
  {
    GroupByQuery query = newTestQuery()
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setVirtualColumns(UsageTestUtils.EXPR_COLUMN,
                           UsageTestUtils.CONSTANT_COLUMN)
        .build();
    UsageTestUtils.verify(runner, query, 4185);
  }
}
