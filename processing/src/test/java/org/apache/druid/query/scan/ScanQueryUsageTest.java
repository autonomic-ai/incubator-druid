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

package org.apache.druid.query.scan;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UsageTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ScanQueryUsageTest
{

  private static final ScanQueryQueryToolChest toolChest = new ScanQueryQueryToolChest(
      new ScanQueryConfig(),
      DefaultGenericQueryMetricsFactory.instance()
  );

  @Parameterized.Parameters(name = "{0}, legacy = {1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new ScanQueryRunnerFactory(
                toolChest,
                new ScanQueryEngine()
            )
        ),
        ImmutableList.of(false, true)
    );
  }

  private final QueryRunner runner;
  private final boolean legacy;

  public ScanQueryUsageTest(final QueryRunner runner, final boolean legacy)
  {
    this.runner = runner;
    this.legacy = legacy;
  }

  private ScanQuery.ScanQueryBuilder newTestQuery()
  {
    return ScanQuery.newScanQueryBuilder()
                    .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                    .filters(UsageTestUtils.FILTER)
                    .legacy(legacy);
  }

  @Test
  public void testScanQuery()
  {
    ScanQuery query = newTestQuery()
        .columns(UsageTestUtils.COLUMNS)
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 8370);
  }

  @Test
  public void testScanQueryWithEmptyColumn()
  {
    String[] columnsWithEmptyColumn = new String[UsageTestUtils.COLUMNS.length + 1];
    System.arraycopy(UsageTestUtils.COLUMNS, 0, columnsWithEmptyColumn, 0, UsageTestUtils.COLUMNS.length);
    columnsWithEmptyColumn[columnsWithEmptyColumn.length - 1] = UsageTestUtils.NULL_COLUMN;

    ScanQuery query = newTestQuery()
        .columns(columnsWithEmptyColumn)
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 8370);
  }

  @Test
  public void testScanQueryWithNullColumn()
  {
    String[] columnsWithNullColumn = new String[UsageTestUtils.COLUMNS.length + 2];
    System.arraycopy(UsageTestUtils.COLUMNS, 0, columnsWithNullColumn, 0, UsageTestUtils.COLUMNS.length);
    columnsWithNullColumn[columnsWithNullColumn.length - 2] = "foo";
    columnsWithNullColumn[columnsWithNullColumn.length - 1] = "bar";

    ScanQuery query = newTestQuery()
        .columns(columnsWithNullColumn)
        .virtualColumns(UsageTestUtils.EXPR_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 8370);
  }

  @Test
  public void testScanQueryWithConstantVirtualColumn()
  {
    ScanQuery query = newTestQuery()
        .columns(UsageTestUtils.COLUMNS)
        .virtualColumns(UsageTestUtils.EXPR_COLUMN,
                        UsageTestUtils.CONSTANT_COLUMN)
        .build();

    UsageTestUtils.verify(runner, query, 8370);
  }
}
