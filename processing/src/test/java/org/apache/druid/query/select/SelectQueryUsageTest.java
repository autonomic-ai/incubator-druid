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

package org.apache.druid.query.select;

import com.google.common.base.Suppliers;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UsageTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class SelectQueryUsageTest
{
  private static final int numTestRows = 100;
  private static final boolean DEFAULT_FROM_NEXT = true;

  private static final SelectQueryConfig config = new SelectQueryConfig(true);
  {
    config.setEnableFromNextDefault(DEFAULT_FROM_NEXT);
  }

  private static final SelectQueryQueryToolChest toolChest = new SelectQueryQueryToolChest(
      new DefaultObjectMapper(),
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
      Suppliers.ofInstance(config)
  );

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new SelectQueryRunnerFactory(
                toolChest,
                new SelectQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        ),
        Arrays.asList(false, true)
    );
  }

  private final QueryRunner runner;
  private final boolean descending;

  public SelectQueryUsageTest(QueryRunner runner, boolean descending)
  {
    this.runner = runner;
    this.descending = descending;
  }

  private Druids.SelectQueryBuilder newTestQuery()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                 .intervals(QueryRunnerTestHelper.fullOnInterval)
                 .granularity(QueryRunnerTestHelper.allGran)
                 .filters(UsageTestUtils.FILTER)
                 .pagingSpec(PagingSpec.newSpec(10))
                 .descending(descending);
  }

  @Test
  public void testSelectQuery()
  {
    SelectQuery query = newTestQuery()
        .metrics(Arrays.asList(UsageTestUtils.COLUMNS))
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, numTestRows);
  }

  @Test
  public void testSlectQueryWithEmptyColumn()
  {
    String[] columnsWithEmptyColumn = new String[UsageTestUtils.COLUMNS.length + 1];
    System.arraycopy(UsageTestUtils.COLUMNS, 0, columnsWithEmptyColumn, 0, UsageTestUtils.COLUMNS.length);
    columnsWithEmptyColumn[columnsWithEmptyColumn.length - 1] = UsageTestUtils.NULL_COLUMN;

    SelectQuery query = newTestQuery()
        .metrics(Arrays.asList(columnsWithEmptyColumn))
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, numTestRows);
  }

  @Test
  public void testSelectQueryWithNullColumn()
  {
    String[] columnsWithNullColumn = new String[UsageTestUtils.COLUMNS.length + 2];
    System.arraycopy(UsageTestUtils.COLUMNS, 0, columnsWithNullColumn, 0, UsageTestUtils.COLUMNS.length);
    columnsWithNullColumn[columnsWithNullColumn.length - 2] = "foo";
    columnsWithNullColumn[columnsWithNullColumn.length - 1] = "bar";

    SelectQuery query = newTestQuery()
        .metrics(Arrays.asList(columnsWithNullColumn))
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, numTestRows);
  }

  @Test
  public void testSelectQueryWithConstantVirtualColumn()
  {
    SelectQuery query = newTestQuery()
        .metrics(Arrays.asList(UsageTestUtils.COLUMNS))
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.CONSTANT_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, numTestRows);
  }
}
