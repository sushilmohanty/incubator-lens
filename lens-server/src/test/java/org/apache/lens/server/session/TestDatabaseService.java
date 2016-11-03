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
package org.apache.lens.server.session;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensAllApplicationJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.testng.annotations.*;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Test(groups = "restart-test", dependsOnGroups = "unit-test")
public class TestDatabaseService extends LensAllApplicationJerseyTest {
  DatabaseResourceService dbResourceSvc;
  LensSessionHandle lensSessionId;
  String rootPath = null;
  Configuration conf;
  MiniDFSCluster cluster;
  FileSystem fs;
  File testPath;
  private static final String CLUSTER_1 = "cluster1";

  private void assertSuccess(APIResult result) {
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED, String.valueOf(result));
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @BeforeClass
  public void createHDFSMiniCluster() throws Exception {
    try {
      testPath = new File("./target/hdfs/").getAbsoluteFile();
      System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
      conf = new HdfsConfiguration();
      File testDataCluster1 = new File(testPath, CLUSTER_1);
      String c1Path = testDataCluster1.getAbsolutePath();
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
      cluster = new MiniDFSCluster.Builder(conf).build();
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      log.error("Error in setting up mini HDFS cluster", e);
    }
  }

  @BeforeMethod
  public void create() throws Exception {
    rootPath = getServerConf().get(LensConfConstants.DATABASE_RESOURCE_DIR);
    dbResourceSvc = LensServices.get().getService(DatabaseResourceService.NAME);
    lensSessionId = dbResourceSvc.openSession("foo", "bar", new HashMap<String, String>());
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @AfterClass
  public void dropHDFSMiniCluster() throws Exception {
    cluster.shutdown();
    FileUtil.fullyDelete(testPath);
  }

  @AfterMethod
  public void drop() throws Exception {
    dbResourceSvc.closeSession(lensSessionId);
  }

  private String getCurrentDatabase(MediaType mediaType) throws Exception {
    return target().path("session").path("databases/current")
        .queryParam("sessionid", lensSessionId).request(mediaType).get(String.class);
  }

  private FormDataMultiPart getFormData(MediaType mediaType) {
    FormDataMultiPart mp = null;
    try {
      mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "jar"));

      File file = new File("target/testjars/serde.jar");
      log.debug("uploading file path : " + file.getAbsolutePath() + "|size = " + file.length());
      final FormDataContentDisposition dispo = FormDataContentDisposition
          .name("file")
          .fileName("test.jar")
          .size(file.length())
          .build();

      FileDataBodyPart filePart = new FileDataBodyPart("file", file);
      filePart.setContentDisposition(dispo);
      mp.bodyPart(filePart);
    } catch (Exception e) {
      log.error("Error in getting form data", e);
    }
    return mp;
  }

  /**
   * Test case when no db folder exists
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUpload(MediaType mediaType) throws Exception {
    String dbName = "db1" + "_" + mediaType.getSubtype();
    // create
    APIResult result = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
            .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
        .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("session").path("databases/resources").
        queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getMessage(), "Add resource succeeded");
  }


  /**
   * Test case when db folder exists & jar_order file present ( Existing flow )
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithJarOrderFileInDbFolder(MediaType mediaType) throws Exception {
    String dbName = "db2" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();

    // create
    APIResult result = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
            .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
        .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();

    File dbFolderJarOrder = new File("target/resources/" + dbName + File.separator + "jar_order");
    dbFolderJarOrder.createNewFile();


    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("session").path("databases/resources").
        queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertTrue(resultUpd.getMessage().startsWith("This database db2_")
        && resultUpd.getMessage().endsWith("does not support jar upload!"));

    cleanUp(dbFolder);
  }

  /**
   * Test case when db folder exists & jar_order file NOT present & db_uploading.jar NOT present.
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithNoJarOrderInFolder(MediaType mediaType) throws Exception {
    String dbName = "db4" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();

    // create
    APIResult result = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
            .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
        .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();

    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("session").path("databases/resources").
        queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getStatus(), APIResult.Status.SUCCEEDED);

    cleanUp(dbFolder);
  }

  /**
   * Test case when db folder exists & jar_order file NOT present & db_uploading.jar NOT present and with existing jars.
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithExistingJarsInFolder(MediaType mediaType) throws Exception {
    String dbName = "db5" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();

    // create
    APIResult result = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
            .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
        .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();

    File dbFolderJarOrder1 = new File("target/resources/" + dbName + File.separator + dbName + "_1.jar");
    dbFolderJarOrder1.createNewFile();

    File dbFolderJarOrder2 = new File("target/resources/" + dbName + File.separator + dbName + "_2.jar");
    dbFolderJarOrder2.createNewFile();

    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("session").path("databases/resources").
        queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getStatus(), APIResult.Status.SUCCEEDED);

    cleanUp(dbFolder);
  }

  /**
   * Test case when db folder exists & jar_order file NOT present & db_uploading.jar NOT present
   * and with existing jars in HDFS
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithExistingJarsInFolderAndCopyToHDFS(MediaType mediaType) throws Exception {
    String hdfsDir = fs.getUri().toString() + "/" + "testing/database/jar";
    fs.mkdirs(new Path(hdfsDir));
    HiveConf conf = getServerConf();
    conf.set(LensConfConstants.DATABASE_RESOURCE_DIR, hdfsDir);
    conf.set(LensConfConstants.DATABASE_LOCAL_RESOURCE_DIR, System.getProperty("user.dir") + "/"
        + "target/resources");
    restartLensServer(conf);
    rootPath = getServerConf().get(LensConfConstants.DATABASE_RESOURCE_DIR);
    String dbName = "db5" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();
    // create
    APIResult result = target().path("metastore").path("databases")
        .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
            .class);
    assertNotNull(result);
    assertSuccess(result);
    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");
    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
        .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();
    Path finalHDFSPath = new Path(hdfsDir, dbName);
    if (!fs.exists(finalHDFSPath)) {
      fs.mkdirs(finalHDFSPath);
    }

    File dbFolderJarOrder1 = new File("target/resources/" + dbName + File.separator + dbName + "_1.jar");
    dbFolderJarOrder1.createNewFile();

    File dbFolderJarOrder2 = new File("target/resources/" + dbName + File.separator + dbName + "_2.jar");
    dbFolderJarOrder2.createNewFile();

    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);

    APIResult resultUpd = target().path("session").path("databases/resources").
        queryParam("sessionid", lensSessionId).request(mediaType)
        .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getStatus(), APIResult.Status.SUCCEEDED);

    //New file with  dbName + "_3.jar" should be copied to HDFS
    assertTrue(fs.exists(new Path(hdfsDir + "/" + dbName +  File.separator + dbName + "_3.jar")));
    cleanUp(dbFolder);
  }

  private void cleanUp(File f) {
    try {
      FileUtils.deleteDirectory(f);
    } catch (Exception e) {
      log.error("Error cleaning directory", e);
    }
  }
}
