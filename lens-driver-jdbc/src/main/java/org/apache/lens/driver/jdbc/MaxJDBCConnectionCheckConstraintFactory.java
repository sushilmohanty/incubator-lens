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

package org.apache.lens.driver.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.api.common.ConfigBasedObjectCreationFactory;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;

import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.ConnectionPoolProperties.JDBC_POOL_MAX_SIZE;


public class MaxJDBCConnectionCheckConstraintFactory implements
    ConfigBasedObjectCreationFactory<QueryLaunchingConstraint> {

  @Override
  public MaxJDBCConnectionCheckConstraint create(Configuration conf) {
    final int poolMaxSize = Integer.parseInt(conf.get(JDBC_POOL_MAX_SIZE.getConfigKey()));

    return new MaxJDBCConnectionCheckConstraint(poolMaxSize);
  }

}
