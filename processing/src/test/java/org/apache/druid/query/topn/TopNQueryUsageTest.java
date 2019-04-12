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

package org.apache.druid.query.topn;

import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.UsageTestUtils;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.column.ValueType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class TopNQueryUsageTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    List<QueryRunner<Result<TopNResultValue>>> retVal = TopNQueryRunnerTest.queryRunners();
    List<Object[]> parameters = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      for (QueryRunner<Result<TopNResultValue>> firstParameter : retVal) {
        Object[] params = new Object[5];
        params[0] = firstParameter;
        params[1] = (i & 1) != 0;
        params[2] = (i & 2) != 0;
        params[3] = (i & 4) != 0;
        params[4] = (i & 8) != 0;
        parameters.add(params);
      }
    }
    return parameters;
  }

  private final QueryRunner<Result<TopNResultValue>> runner;

  public TopNQueryUsageTest(
      QueryRunner<Result<TopNResultValue>> runner,
      boolean specializeGeneric1AggPooledTopN,
      boolean specializeGeneric2AggPooledTopN,
      boolean specializeHistorical1SimpleDoubleAggPooledTopN,
      boolean specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN
  )
  {
    this.runner = runner;
    PooledTopNAlgorithm.setSpecializeGeneric1AggPooledTopN(specializeGeneric1AggPooledTopN);
    PooledTopNAlgorithm.setSpecializeGeneric2AggPooledTopN(specializeGeneric2AggPooledTopN);
    PooledTopNAlgorithm.setSpecializeHistorical1SimpleDoubleAggPooledTopN(
        specializeHistorical1SimpleDoubleAggPooledTopN
    );
    PooledTopNAlgorithm.setSpecializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN(
        specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN
    );
  }

  private TopNQueryBuilder newTestQuery()
  {
    return new TopNQueryBuilder().dataSource(QueryRunnerTestHelper.dataSource)
                                 .dimension(new DefaultDimensionSpec("quality", "quality", ValueType.FLOAT))
                                 .granularity(QueryRunnerTestHelper.allGran)
                                 .intervals(QueryRunnerTestHelper.fullOnInterval)
                                 .aggregators(Collections.singletonList(UsageTestUtils.AGGREGATOR_FACTORY))
                                 .metric(UsageTestUtils.AGGREGATOR_FACTORY.getName())
                                 .threshold(10)
                                 .filters(UsageTestUtils.FILTER);
  }

  @Test
  public void testTopNQuery()
  {
    TopNQuery query = newTestQuery()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 4185);
  }

  @Test
  public void testTopNQueryWithEmptyColumn()
  {
    TopNQuery query = newTestQuery()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.EMPTY_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 4185);
  }

  @Test
  public void testTopNQueryWithNullColumn()
  {
    TopNQuery query = newTestQuery()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.FOO_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 4185);
  }

  @Test
  public void testTopnNQueryWithConstantVirtualColumn()
  {
    TopNQuery query = newTestQuery()
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.CONSTANT_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 4185);
  }
}
