/**
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
package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.TWO_MONTHS_RANGE_UPTO_DAYS;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;
import static org.testng.Assert.*;

import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestUnionAndJoinCandidates extends TestQueryRewrite {

  private Configuration testConf;

  @BeforeTest
  public void setupDriver() throws Exception {
    testConf = LensServerAPITestUtil.getConfiguration(
        DISABLE_AUTO_JOINS, false,
        ENABLE_SELECT_TO_GROUPBY, true,
        ENABLE_GROUP_BY_TO_SELECT, true,
        DISABLE_AGGREGATE_RESOLVER, false,
        ENABLE_STORAGES_UNION, true);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(testConf);
  }

  @Test
  public void testUnionCandidateQuery() throws ParseException, LensException {
    try {
      Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
          //Supported storage
          CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1",
          // Storage tables
          getValidStorageTablesKey("union_join_ctx_fact1"), "C1_union_join_ctx_fact1",
          getValidStorageTablesKey("union_join_ctx_fact2"), "C1_union_join_ctx_fact2",
          getValidStorageTablesKey("union_join_ctx_fact3"), "C1_union_join_ctx_fact3",
          // Update periods
          getValidUpdatePeriodsKey("union_join_ctx_fact1", "C1"), "DAILY",
          getValidUpdatePeriodsKey("union_join_ctx_fact2", "C1"), "DAILY",
          getValidUpdatePeriodsKey("union_join_ctx_fact3", "C1"), "DAILY");
      // Test union candidate
      String colsSelected = " union_join_ctx_cityid , union_join_ctx_cityname , union_join_ctx_notnullcityid, "
          + " union_join_ctx_zipcode, sum(union_join_ctx_msr1), sum(union_join_ctx_msr1) + 10 ";
      String whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
          + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
      String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond, conf);
      String expectedOuterSelect = "SELECT (uc2.alias0), (uc2.alias1), (uc2.alias2), (uc2.alias3), sum((uc2.alias4)), "
          + "(sum((uc2.alias4)) + 10)";
      String expectedInnerSelect = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, "
          + "(cubecityjoinunionctx.name) as `alias1`, case  when (basecube.union_join_ctx_cityid) is null then 0 "
          + "else (basecube.union_join_ctx_cityid) end as `alias2`, (basecube.union_join_ctx_zipcode) as `alias3`, "
          + "sum((basecube.union_join_ctx_msr1)) as `alias4`";
      String expectedOuterGroupBy = "GROUP BY (uc2.alias0), (uc2.alias1), (uc2.alias2), (uc2.alias3)";
      String expectedInnerGroupBy = "GROUP BY (basecube.union_join_ctx_cityid), (cubecityjoinunionctx.name), "
          + "case  when (basecube.union_join_ctx_cityid) is null then 0 else (basecube.union_join_ctx_cityid) end, "
          + "(basecube.union_join_ctx_zipcode) )";
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedOuterSelect.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedInnerSelect.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedOuterGroupBy.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedInnerGroupBy.toLowerCase()   ));

    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }
  @Test
  public void testJoinCandidateQuery() throws ParseException, LensException {
    try {
      Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
          //Supported storage
          CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1",
          // Storage tables
          getValidStorageTablesKey("union_join_ctx_fact1"), "C1_union_join_ctx_fact1",
          getValidStorageTablesKey("union_join_ctx_fact2"), "C1_union_join_ctx_fact2",
          getValidStorageTablesKey("union_join_ctx_fact3"), "C1_union_join_ctx_fact3",
          // Update periods
          getValidUpdatePeriodsKey("union_join_ctx_fact1", "C1"), "DAILY",
          getValidUpdatePeriodsKey("union_join_ctx_fact2", "C1"), "DAILY",
          getValidUpdatePeriodsKey("union_join_ctx_fact3", "C1"), "DAILY");
      // Test union candidate
      //String colsSelected = " union_join_ctx_cityid , union_join_ctx_cityname , union_join_ctx_notnullcityid, "
      //    + " union_join_ctx_zipcode, sum(union_join_ctx_msr1), sum(union_join_ctx_msr2), "
      //    + " sum(union_join_ctx_msr1) + 10, union_join_ctx_non_zero_msr2_sum";
      //String colsSelected = " union_join_ctx_cityid, sum(union_join_ctx_msr1) ";


/*

      // Query with non projected measure in having clause.
      String colsSelected = "union_join_ctx_cityid, sum(union_join_ctx_msr2) ";
      String having = " having sum(union_join_ctx_msr1) > 100";
      String whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
          + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
      String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond + having, conf);
      String expectedSelect1 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, 0.0 as `alias1`, "
          + "sum((basecube.union_join_ctx_msr1)) as `alias2` FROM TestQueryRewrite.c1_union_join_ctx_fact1 basecube "
          + "WHERE ((((basecube.union_join_ctx_zipcode) = 'a') and ((basecube.union_join_ctx_cityid) = 'b')";
      String expectedSelect2 = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, 0.0 as `alias1`, "
          + "sum((basecube.union_join_ctx_msr1)) as `alias2` FROM TestQueryRewrite.c1_union_join_ctx_fact2 basecube "
          + "WHERE ((((basecube.union_join_ctx_zipcode) = 'a') and ((basecube.union_join_ctx_cityid) = 'b')";
      String expectedSelect3 = " SELECT (basecube.union_join_ctx_cityid) as `alias0`, "
          + "sum((basecube.union_join_ctx_msr2)) as `alias1`, sum(0.0) as `alias2` "
          + "FROM TestQueryRewrite.c1_union_join_ctx_fact3 basecube WHERE ((((basecube.union_join_ctx_zipcode) = 'a') "
          + "and ((basecube.union_join_ctx_cityid) = 'b')";
      String outerHaving = "HAVING (sum((jc0.alias2)) > 100)";
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedSelect1.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedSelect2.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedSelect3.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(outerHaving.toLowerCase()));
*/
      // Query with measure and aggregate expression
      String colsSelected = " union_join_ctx_cityid , union_join_ctx_cityname , union_join_ctx_notnullcityid, "
          + " union_join_ctx_zipcode, sum(union_join_ctx_msr1), sum(union_join_ctx_msr2), "
          + " sum(union_join_ctx_msr1) + 10, union_join_ctx_non_zero_msr2_sum";

      String whereCond = " union_join_ctx_zipcode = 'a' and union_join_ctx_cityid = 'b' and "
          + "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
      String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond , conf);



      //String rewrittenQuery = rewrite("select " + colsSelected + " from basecube where " + whereCond , conf);
      String expectedOuterSelect = "SELECT (uc2.alias0), (uc2.alias1), (uc2.alias2), (uc2.alias3), sum((uc2.alias4)), "
          + "(sum((uc2.alias4)) + 10)";
      String expectedInnerSelect = "SELECT (basecube.union_join_ctx_cityid) as `alias0`, "
          + "(cubecityjoinunionctx.name) as `alias1`, case  when (basecube.union_join_ctx_cityid) is null then 0 "
          + "else (basecube.union_join_ctx_cityid) end as `alias2`, (basecube.union_join_ctx_zipcode) as `alias3`, "
          + "sum((basecube.union_join_ctx_msr1)) as `alias4`";
      String expectedOuterGroupBy = "GROUP BY (uc2.alias0), (uc2.alias1), (uc2.alias2), (uc2.alias3)";
      String expectedInnerGroupBy = "GROUP BY (basecube.union_join_ctx_cityid), (cubecityjoinunionctx.name), "
          + "case  when (basecube.union_join_ctx_cityid) is null then 0 else (basecube.union_join_ctx_cityid) end, "
          + "(basecube.union_join_ctx_zipcode) )";
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedOuterSelect.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedInnerSelect.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedOuterGroupBy.toLowerCase()));
      assertTrue(rewrittenQuery.toLowerCase().contains(expectedInnerGroupBy.toLowerCase()   ));

    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }
}
