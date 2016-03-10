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

package org.apache.lens.cube.metadata;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Lists;

import lombok.Getter;

public class CubeSegmentation extends AbstractCubeTable {

  @Getter
  private String baseCube;
  private Set<String> candidateCubes;
  private static final List<FieldSchema> COLUMNS = new ArrayList<>();

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public CubeSegmentation(Table hiveTable) {
    super(hiveTable);
    this.candidateCubes = getCandidateCubes(getName(), getProperties());
    this.baseCube = getCubeName(getName(), getProperties());
  }

  public CubeSegmentation(String cubeName, String segmentName, Set<String> candidateCubeNames) {
    this(cubeName, segmentName, candidateCubeNames, 0L);
  }

  public CubeSegmentation(String cubeName, String segmentName, Set<String> candidateCubeNames, double weight) {
    this(cubeName, segmentName, candidateCubeNames, weight, new HashMap<String, String>());
  }

  public CubeSegmentation(String baseCube, String segmentName, Set<String> candidateCubeNames,
                          double weight, Map<String, String> properties) {
    super(segmentName, COLUMNS, properties, weight);
    this.baseCube = baseCube;
    this.candidateCubes = candidateCubeNames;
    addProperties();
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    addBaseCubeName(getName(), getProperties(), baseCube);
    addCandidateCubeProperties(getName(), getProperties(), candidateCubes);
  }

  private static void addBaseCubeName(String segName, Map<String, String> props, String cubeName) {
    props.put(MetastoreUtil.getSegmentationCubeNameKey(segName), cubeName);
  }

  private static void addCandidateCubeProperties(String name, Map<String, String> props,
                                                Set<String> candidateCubeNames){
    if (candidateCubeNames != null){
      props.put(MetastoreUtil.getSegmentationCubesListKey(name), MetastoreUtil.getStr(candidateCubeNames));
    }
  }

  private static Set<String> getCandidateCubes(String name, Map<String, String> props) {
    Set<String> candidateCubes = new HashSet<>();
    String cubesString = props.get(MetastoreUtil.getSegmentationCubesListKey(name));
    if (!StringUtils.isBlank(cubesString)) {
      String[] cubes = cubesString.split(",");
      for (String cube : cubes) {
        candidateCubes.add(cube);
      }
    }
    return candidateCubes;
  }

  public Set<String> getCandidateCubes() {
    return candidateCubes;
  }

  public void addCandidateCube(String candidateCube) {
    if (!candidateCubes.contains(candidateCube)) {
      candidateCubes.add(candidateCube);
      addCandidateCubeProperties(getName(), getProperties(), candidateCubes);
    }
  }

  public void dropCandidateCube(String candidateCube) {
    if (candidateCubes.contains(candidateCube)) {
      candidateCubes.remove(candidateCube);
      addCandidateCubeProperties(getName(), getProperties(), candidateCubes);
    }
  }

  public void alterCandidateCube(Set<String> candCubes) {
    if (!candidateCubes.equals(candCubes)) {
      candidateCubes.clear();
      candidateCubes.addAll(candCubes);
      addCandidateCubeProperties(getName(), getProperties(), candidateCubes);
    }
  }

  public void alterBaseCubeName(String cubeName) {
    this.baseCube = cubeName;
    addBaseCubeName(getName(), getProperties(), cubeName);
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.SEGMENTATION;
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }

  public Date getAbsoluteStartTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_ABSOLUTE_START_TIME, false, true);
  }

  public Date getRelativeStartTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_RELATIVE_START_TIME, true, true);
  }

  public Date getStartTime() {
    return Collections.max(Lists.newArrayList(getRelativeStartTime(), getAbsoluteStartTime()));
  }

  public Date getAbsoluteEndTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_ABSOLUTE_END_TIME, false, false);
  }

  public Date getRelativeEndTime() {
    return getDateFromProperty(MetastoreConstants.SEGMENTATION_RELATIVE_END_TIME, true, false);
  }

  public Date getEndTime() {
    return Collections.min(Lists.newArrayList(getRelativeEndTime(), getAbsoluteEndTime()));
  }

  static String getCubeName(String segName, Map<String, String> props) {
    return props.get(MetastoreUtil.getSegmentationCubeNameKey(segName));
  }

}
