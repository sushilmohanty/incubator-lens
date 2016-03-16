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

public class CubeSegmentation extends AbstractCubeTable {

  private String cubeName;
  private Set<String> cubeSegments;
  private static final List<FieldSchema> COLUMNS = new ArrayList<>();

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public CubeSegmentation(Table hiveTable) {
    super(hiveTable);
    this.cubeSegments = getCubeSegments(getName(), getProperties());
    this.cubeName = getCubeName(getName(), getProperties());
  }

  public CubeSegmentation(String cubeName, String segmentName, Set<String> cubeSegments) {
    this(cubeName, segmentName, cubeSegments, 0L);
  }

  public CubeSegmentation(String cubeName, String segmentName, Set<String> cubeSegments, double weight) {
    this(cubeName, segmentName, cubeSegments, weight, new HashMap<String, String>());
  }

  public CubeSegmentation(String baseCube, String segmentName, Set<String> cubeSegments,
                          double weight, Map<String, String> properties) {
    super(segmentName, COLUMNS, properties, weight);
    this.cubeName = baseCube;
    this.cubeSegments = cubeSegments;
    addProperties();
    updateCubeSegmentsProperties();
  }

  public void updateCubeSegmentsProperties() {
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getSegmentationCubesListKey(getName()),
            MetastoreUtil.getNamedSetFromStringSet(cubeSegments));
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    addCubeName(getName(), getProperties(), cubeName);
    addCubeSegmentProperties(getName(), getProperties(), cubeSegments);
  }

  private static void addCubeName(String segName, Map<String, String> props, String cubeName) {
    props.put(MetastoreUtil.getSegmentationCubeNameKey(segName), cubeName);
  }

  private static void addCubeSegmentProperties(String name, Map<String, String> props,
                                                Set<String> cubeSegments){
    if (cubeSegments != null){
      props.put(MetastoreUtil.getSegmentationCubesListKey(name), MetastoreUtil.getStr(cubeSegments));
    }
  }

  private static Set<String> getCubeSegments(String name, Map<String, String> props) {
    Set<String> patricipatingCubes = new HashSet<>();
    String cubesString = props.get(MetastoreUtil.getSegmentationCubesListKey(name));
    if (!StringUtils.isBlank(cubesString)) {
      String[] cubes = cubesString.split(",");
      for (String cube : cubes) {
        patricipatingCubes.add(cube);
      }
    }
    return patricipatingCubes;
  }

  public Set<String> getCubeSegments() {
    return cubeSegments;
  }

  public void addCubeSegment(String cubeSeg) {
    if (!cubeSegments.contains(cubeSeg)) {
      cubeSegments.add(cubeSeg);
      addCubeSegmentProperties(getName(), getProperties(), cubeSegments);
    }
  }

  public void dropCubeSegment(String cubeSeg) {
    if (cubeSegments.contains(cubeSeg)) {
      cubeSegments.remove(cubeSeg);
      addCubeSegmentProperties(getName(), getProperties(), cubeSegments);
    }
  }

  public void alterCubeSegment(Set<String> cubeSegs) {
    if (!cubeSegments.equals(cubeSegs)) {
      cubeSegments.clear();
      cubeSegments.addAll(cubeSegs);
      addCubeSegmentProperties(getName(), getProperties(), cubeSegments);
    }
  }

  public void alterBaseCubeName(String cubeName) {
    this.cubeName = cubeName;
    addCubeName(getName(), getProperties(), cubeName);
  }

  public String getBaseCube() {
    return cubeName;
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
