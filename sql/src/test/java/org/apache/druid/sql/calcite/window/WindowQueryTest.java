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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import static org.junit.Assert.assertTrue;

public class WindowQueryTest extends CalciteTestBase
{
  private static final Logger log = new Logger(WindowQueryTest.class);

  private static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();
  private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private SpecificSegmentsQuerySegmentWalker walker = null;

  public abstract static class MockWindowAggregatorFactory extends AggregatorFactory
  {
    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory)
    {
      return null;
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
    {
      return null;
    }

    @Override
    public Comparator getComparator()
    {
      return null;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs)
    {
      return null;
    }

    @Override
    public AggregatorFactory getCombiningFactory()
    {
      return null;
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns()
    {
      return null;
    }

    @Override
    public Object deserialize(Object object)
    {
      return null;
    }

    @Override
    public String getTypeName()
    {
      return null;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object)
    {
      return null;
    }

    @Override
    public List<String> requiredFields()
    {
      return null;
    }

    @Override
    public int getMaxIntermediateSize()
    {
      return 0;
    }

    @Override
    public byte[] getCacheKey()
    {
      return new byte[0];
    }
  }

  public static class MockLeadAggregatorFactory extends MockWindowAggregatorFactory
  {
    private String name;
    private String fieldName;

    public MockLeadAggregatorFactory(String name, String fieldName)
    {
      this.name = name;
      this.fieldName = fieldName;
    }

    @Override
    public String getName()
    {
      return name;
    }
  }

  public static class MockLagAggregatorFactory extends MockWindowAggregatorFactory
  {
    private String name;
    private String fieldName;

    public MockLagAggregatorFactory(String name, String fieldName)
    {
      this.name = name;
      this.fieldName = fieldName;
    }

    @Override
    public String getName()
    {
      return name;
    }
  }

  public static class SqlWindowAggregator implements SqlAggregator
  {
    private SqlKind sqlKind;

    public SqlWindowAggregator(SqlKind sqlKind)
    {
      this.sqlKind = sqlKind;
    }

    public static List<DruidExpression> getArgumentsForWindowAggregator(
        PlannerContext plannerContext, RowSignature rowSignature, AggregateCall call,
        Project project
    )
    {
      return (List) call.getArgList().stream().map((i) -> {
        return Expressions.fromFieldAccess(rowSignature, project, i.intValue());
      }).map((rexNode) -> {
        return toDruidExpressionForWindowAggregator(plannerContext, rowSignature, rexNode);
      }).collect(Collectors.toList());
    }

    private static DruidExpression toDruidExpressionForWindowAggregator(
        PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode
    )
    {
      DruidExpression druidExpression =
          Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
      if (druidExpression == null) {
        return null;
      } else {
        return !druidExpression.isSimpleExtraction() || druidExpression.isDirectColumnAccess()
               ? druidExpression
               : druidExpression.map((simpleExtraction) -> {
                 return null;
               }, Function.identity());
      }
    }

    public static AggregatorFactory createWindowAggregatorFactory(
        SqlKind sqlKind,
        String name,
        String fieldName
    )
    {
      switch (sqlKind) {
        case LEAD:
          return new MockLeadAggregatorFactory(name, fieldName);

        case LAG:
          return new MockLagAggregatorFactory(name, fieldName);
        default:
          throw new ISE("Do not support window aggregator %s", sqlKind);
      }
    }

    @Override
    public SqlAggFunction calciteFunction()
    {
      return new SqlLeadLagAggFunction(sqlKind);
    }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexBuilder rexBuilder,
        String s,
        AggregateCall aggregateCall,
        Project project,
        List<Aggregation> list,
        boolean b
    )
    {
      final List<DruidExpression> arguments = getArgumentsForWindowAggregator(
          plannerContext,
          rowSignature,
          aggregateCall,
          project
      );

      if (arguments == null) {
        return null;
      }

      final DruidExpression arg = Iterables.getOnlyElement(arguments);

      final String fieldName;

      if (arg.isDirectColumnAccess()) {
        fieldName = arg.getDirectColumn();
      } else {
        throw new ISE("must use a column in LEAD/LAG");
      }

      return Aggregation.create(createWindowAggregatorFactory(sqlKind, s, fieldName));
    }
  }

  public static class MockSqlLag extends SqlWindowAggregator
  {
    public MockSqlLag()
    {
      super(SqlKind.LAG);
    }
  }

  public static class MockSqlLead extends SqlWindowAggregator
  {
    public MockSqlLead()
    {
      super(SqlKind.LEAD);
    }
  }

  @BeforeClass
  public static void setUpClass()
  {
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    conglomerate = conglomerateCloserPair.lhs;
    resourceCloser = conglomerateCloserPair.rhs;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder.newFolder());
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  private void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final AuthenticationResult authenticationResult,
      final int expectedFieldCount
  ) throws Exception
  {
    log.info("SQL: %s", sql);
    final InProcessViewManager viewManager = new InProcessViewManager(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
    final DruidSchema druidSchema = CalciteTests.createMockSchema(conglomerate, walker, plannerConfig, viewManager);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker);
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(
            new MockSqlLag(),
            new MockSqlLead()
        ), new HashSet<>());
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();

    final PlannerFactory plannerFactory = new PlannerFactory(
        druidSchema,
        systemSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        operatorTable,
        macroTable,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper()
    );

    try (DruidPlanner planner = plannerFactory.createPlanner(queryContext)) {
      final PlannerResult plan = planner.plan(sql, authenticationResult);
      assertTrue(plan != null);
      assertTrue(plan.rowType().getFieldCount() == expectedFieldCount);
    }
  }

  @Test
  public void testWindowQuery_succeed() throws Exception
  {
    Map<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(DruidQuery.WINDOW_QUERY_CONTEXT_NAME,
                     DruidQuery.WINDOW_QUERY_TEST_CONTEXT + ":org.apache.druid.sql.calcite.window.MockWindowQueryFactory");

    String sql = "SELECT lag(dim1) OVER (), dim1, lead(dim1) OVER () from foo";
    testQuery(PLANNER_CONFIG_DEFAULT, queryContext, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, 3);

    sql = "SELECT lag(dim1) OVER (), dim1, lead(dim2) OVER (), dim2 from foo";
    testQuery(PLANNER_CONFIG_DEFAULT, queryContext, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, 4);

    sql = "SELECT lag(dim1) OVER (partition by cnt), dim1, lead(dim2) OVER (partition by cnt), dim2, cnt from foo";
    testQuery(PLANNER_CONFIG_DEFAULT, queryContext, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, 5);
  }

  @Test
  public void testWindowQuery_fail() throws Exception
  {
    Map<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(
        DruidQuery.WINDOW_QUERY_CONTEXT_NAME,
        DruidQuery.WINDOW_QUERY_TEST_CONTEXT + ":org.apache.druid.sql.calcite.window.MockWindowQueryFactory"
    );

    String sql;
    boolean failed = false;

    // No `OVER` clause
    try {
      sql = "SELECT lag(dim1), dim1, lead(dim1) OVER () from foo";
      testQuery(PLANNER_CONFIG_DEFAULT, queryContext, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, 3);
    }
    catch (ValidationException exp) {
      failed = true;
      assertTrue(exp.getMessage().contains("OVER clause is necessary for window functions"));
    }
    assertTrue(failed);

    // Different partition by clauses
    try {
      failed = false;
      sql = "SELECT lag(dim1) OVER (partition by dim2), dim1, lead(dim1) OVER (), dim2, cnt from foo";
      testQuery(PLANNER_CONFIG_DEFAULT, queryContext, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, 4);
    }
    catch (RelOptPlanner.CannotPlanException exp) {
      failed = true;
    }
    assertTrue(failed);
  }
}
