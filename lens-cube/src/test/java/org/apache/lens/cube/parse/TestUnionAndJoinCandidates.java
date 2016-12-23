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

import static org.testng.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.lens.server.api.LensServerAPITestUtil;
import org.apache.lens.server.api.error.LensException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

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
  public void testRangeCoveringCandidates() throws ParseException, LensException {
    try {
      String prefix = "union_join_ctx_";
      //String cubeName = prefix + "der1";
      String cubeName = "baseCube";
      Configuration conf = LensServerAPITestUtil.getConfigurationWithParams(getConf(),
          //Supported storage
          CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1",
          // Storage tables
          getValidStorageTablesKey(prefix + "fact1"), "C1_" + prefix + "fact1",
          getValidStorageTablesKey(prefix + "fact2"), "C1_" + prefix + "fact2",
          getValidStorageTablesKey(prefix + "fact3"), "C1_" + prefix + "fact3",
          // Update periods
          getValidUpdatePeriodsKey(prefix + "fact1", "C1"), "DAILY",
          getValidUpdatePeriodsKey(prefix + "fact2", "C1"), "DAILY",
          getValidUpdatePeriodsKey(prefix + "fact3", "C1"), "DAILY");
      // Single Storage candidate
      // query with dim attribute, measure , ref dim attribute and expression
      /*
      String colsSelected = prefix + "cityid , " + prefix + "cityname , " + prefix + "notnullcityid, "
          + prefix + "zipcode , " + "sum(" + prefix + "msr1) , " + "sum(" + prefix + "msr2) ";

      String whereCond = prefix + "zipcode = 'a' and " + prefix + "cityid = 'b' and " +
          "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";

      String hqlQuery = rewrite("select " + colsSelected + " from " + cubeName + " where " + whereCond, conf);
      String expected = "SELECT (basecube.union_join_ctx_cityid) as `union_join_ctx_cityid`, "
          + "(cubecityjoinunionctx.name) as `union_join_ctx_cityname`, case  when (basecube.union_join_ctx_cityid)"
          + " is null then 0 else (basecube.union_join_ctx_cityid) end as `union_join_ctx_notnullcityid`, "
          + "(basecube.union_join_ctx_zipcode) as `union_join_ctx_zipcode`, "
          + "sum((basecube.union_join_ctx_msr1)) as `expr5`, sum((basecube.union_join_ctx_msr2)) as `expr6` "
          + "FROM c1_union_join_ctx_fact4 basecube join TestQueryRewrite.c1_citytable cubecityjoinunionctx "
          + "on basecube.union_join_ctx_cityid = cubecityjoinunionctx.id "
          + "and (cubecityjoinunionctx.dt = 'latest') "
          + "WHERE ((basecube.union_join_ctx_zipcode) = 'a') "
          + "and ((basecube.union_join_ctx_cityid) = 'b') "
          + "and time_range_in(d_time, '2016-10-16', '2016-12-16') "
          + "GROUP BY (basecube.union_join_ctx_cityid), (cubecityjoinunionctx.name), "
          + "case  when (basecube.union_join_ctx_cityid) is null then 0 else (basecube.union_join_ctx_cityid) end, "
          + "(basecube.union_join_ctx_zipcode)";

      assertEquals(hqlQuery, expected);
        */
      // Test union candidate
      String  colsSelected = prefix + "cityid , " + prefix + "cityname , " + prefix + "notnullcityid, "
          + prefix + "zipcode , " + "sum(" + prefix + "msr1) ";

       String whereCond = prefix + "zipcode = 'a' and " + prefix + "cityid = 'b' and " +
          "(" + TWO_MONTHS_RANGE_UPTO_DAYS + ")";
       String hqlQuery = rewrite("select " + colsSelected + " from " + cubeName + " where " + whereCond, conf);

    } finally {
      getStorageToUpdatePeriodMap().clear();
    }
  }
}
