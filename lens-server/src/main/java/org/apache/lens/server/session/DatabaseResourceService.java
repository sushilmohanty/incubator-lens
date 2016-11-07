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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.util.ScannedPaths;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.CLIService;

import lombok.extern.slf4j.Slf4j;

/**
 * Service to maintain DB specific static jars. This service is managed by HiveSessionService.
 */
@Slf4j
public class DatabaseResourceService extends BaseLensService {
  public static final String NAME = "database-resources";
  private Map<String, UncloseableClassLoader> classLoaderCache;
  private Map<String, List<LensSessionImpl.ResourceEntry>> dbResEntryMap;
  private Map<String, List<LensSessionImpl.ResourceEntry>> remoteDbResEntryMap;
  private List<LensSessionImpl.ResourceEntry> commonResList;

  /**
   * The metrics service.
   */
  private MetricsService metricsService;

  /**
   * The Constant LOAD_RESOURCES_ERRORS.
   */
  public static final String LOAD_RESOURCES_ERRORS = "total-load-resources-errors";

  /**
   * Incr counter.
   *
   * @param counter the counter
   */
  private void incrCounter(String counter) {
    getMetrics().incrCounter(DatabaseResourceService.class, counter);
  }

  public DatabaseResourceService(CLIService cliService) {
    super(NAME, cliService);
  }

  private String dbResTopDir = null;
  private String dbLocalResTopDir = null;
  private FileSystem dbResourceFs;

  private void downloadJarFilesFromHDFS(String source, String dest) throws Exception {
    Path sourcePath = new Path(source);
    Path destPath = new Path(dest);
    FileSystem sourceFs = FileSystem.newInstance(sourcePath.toUri(), getHiveConf());
    FileSystem destFs = FileSystem.newInstance(destPath.toUri(), getHiveConf());
    //Get all source file's relative path wrt. base source directory
    List<String> allSourceFiles = getAllFileRelativePath(sourcePath, sourceFs);
    for (String fileName : allSourceFiles) {
      Path srcFilePath = new Path(source, fileName);
      String relPathExcludingFileName = fileName.substring(0, fileName.lastIndexOf("/"));
      String file = fileName.substring(fileName.lastIndexOf("/") + 1);
      Path destPathForFileCopy = new Path(destPath, relPathExcludingFileName);
      Path destPathIncludingFile = new Path(destPath, relPathExcludingFileName + file);
      if (destFs.exists(destPathIncludingFile)) {
        if (destFs.getContentSummary(destPathIncludingFile).getLength()
            != sourceFs.getContentSummary(srcFilePath).getLength()) {
          // copy file to local path if the file size is different
          destFs.copyToLocalFile(srcFilePath, destPathIncludingFile);
        }
      } else if (destFs.exists(destPathForFileCopy) && destFs.isDirectory(destPathForFileCopy)) {
        //Folder exists but file is not present
        destFs.copyToLocalFile(srcFilePath, destPathIncludingFile);
      } else {
        // Folder doesn't exist, create folder first then copy the files
        destFs.mkdirs(destPathForFileCopy);
        destFs.copyToLocalFile(srcFilePath, destPathIncludingFile);
      }
    }
  }

  public static List<String> getAllFileRelativePath(Path filePath, FileSystem fs) throws Exception {
    List<String> fileList = new ArrayList<String>();
    FileStatus[] fileStatus = fs.listStatus(filePath);
    for (FileStatus fileStat : fileStatus) {
      if (fileStat.isDirectory()) {
        fileList.addAll(getAllFileRelativePath(fileStat.getPath(), fs));
      } else {
        fileList.add(filePath.toUri().relativize(fileStat.getPath().toUri()).toString());
      }
    }
    return fileList;
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    classLoaderCache = new HashMap<>();
    dbResEntryMap = new HashMap<>();
    remoteDbResEntryMap = new HashMap<>();
    commonResList = new LinkedList<>();
    super.init(hiveConf);
  }

  private boolean isDbResourcePathLocal(String dbResPath) {
    return dbResPath.startsWith("file:") ? true : false;
  }

  @Override
  public synchronized void start() {
    super.start();
    try {
      log.info("Started loading DB specific resources");
      // If the base folder itself is not present, then return as we can't load any jars.
      try {
        if (getHiveConf().getBoolean(LensConfConstants.DB_RESOURCE_COPY_FROM_HDFS,
            LensConfConstants.DEFAULT_DB_RESOURCE_COPY_FROM_HDFS)) {
          dbResTopDir = getHiveConf().get(LensConfConstants.DATABASE_RESOURCE_DIR,
              LensConfConstants.DEFAULT_DATABASE_RESOURCE_DIR);
          dbLocalResTopDir = getHiveConf().get(LensConfConstants.DATABASE_LOCAL_RESOURCE_DIR,
              LensConfConstants.DEFAULT_LOCAL_DATABASE_RESOURCE_DIR);
          Path dbResTopDirPath = new Path(dbResTopDir);
          dbResourceFs = FileSystem.newInstance(dbResTopDirPath.toUri(), getHiveConf());
          if (dbResourceFs.exists(dbResTopDirPath)) {
            if (dbResTopDirPath.getFileSystem(getHiveConf()).getScheme().equals("hdfs")) {
              downloadJarFilesFromHDFS(dbResTopDir, dbLocalResTopDir);
              //resTopDir = localResTopDir;
            }
          } else {
            incrCounter(LOAD_RESOURCES_ERRORS);
            log.error("DB resources base location does not exist", dbResTopDir);
            return;
          }
        }
      } catch (Exception io) {
        log.error("Error locating DB resource base directory", io);
        throw new LensException(io);
      }
      // Map common jars available for all DB folders
      mapCommonResourceEntries();
      // Map DB specific jar
      mapDbResourceEntries();
      // Load all the jars for all the DB's mapped in previous steps
      loadMappedResources();
      log.info("Ended loading DB specific resources");
    } catch (LensException e) {
      incrCounter(LOAD_RESOURCES_ERRORS);
      log.error("Failed to load DB resource mapping, resources must be added explicitly to session.", e);
    }
  }


  @Override
  public synchronized void stop() {
    super.stop();
    classLoaderCache.clear();
    dbResEntryMap.clear();
    remoteDbResEntryMap.clear();
    commonResList.clear();
  }

  private void mapCommonResourceEntries() {
    Path jarFilesPath = isDbResourcePathLocal(dbResTopDir) ?  new Path(dbResTopDir) : new Path(dbLocalResTopDir);
    List<String> jars = new ScannedPaths(jarFilesPath, "jar", false).getFinalPaths();
    for (String resPath : jars) {
      Path jarFilePath = new Path(resPath);
      commonResList.add(new LensSessionImpl.ResourceEntry("jar", jarFilePath.toUri().toString()));
    }

  }

  private void mapDbResourceEntries(String dbName) throws LensException {
    // Read list of databases in
    FileSystem serverFs = null;
    try {
      Path jarFilesPath = isDbResourcePathLocal(dbResTopDir) ? new Path(dbResTopDir) : new Path(dbLocalResTopDir);
      Path resDirPath = new Path(jarFilesPath, dbName);
      serverFs = FileSystem.newInstance(resDirPath.toUri(), getHiveConf());
      resDirPath = serverFs.resolvePath(resDirPath);
      if (!serverFs.exists(resDirPath)) {
        incrCounter(LOAD_RESOURCES_ERRORS);
        log.warn("Database resource location does not exist - {}. Database jars will not be available", resDirPath);
        // remove the db entry
        dbResEntryMap.remove(dbName);
        classLoaderCache.remove(dbName);
        return;
      }
      findResourcesInDir(serverFs, dbName, resDirPath);
      log.debug("Found resources {}", dbResEntryMap);
    } catch (IOException io) {
      log.error("Error getting list of dbs to load resources from", io);
      throw new LensException(io);
    }
  }

  private void mapDbResourceEntries() throws LensException {
    // Read list of databases in
    FileSystem serverFs = null;
    try {
      Path resTopDirPath = isDbResourcePathLocal(dbResTopDir) ? new Path(dbResTopDir) : new Path(dbLocalResTopDir);
      serverFs = FileSystem.newInstance(resTopDirPath.toUri(), getHiveConf());
      resTopDirPath = serverFs.resolvePath(resTopDirPath);
      if (!serverFs.exists(resTopDirPath)) {
        incrCounter(LOAD_RESOURCES_ERRORS);
        log.warn("DB base location does not exist.", dbResTopDir);
        return;
      }
      // Look for db dirs
      String dbName = null;
      for (FileStatus dbDir : serverFs.listStatus(resTopDirPath)) {
        Path dbDirPath = dbDir.getPath();
        if (serverFs.isDirectory(dbDirPath)) {
          dbName = dbDirPath.getName();

          Path dbJarOrderPath = new Path(dbResTopDir, dbName + File.separator + "jar_order");
          if (serverFs.exists(dbJarOrderPath)) {
            // old flow
            mapDbResourceEntries(dbName);
          } else {
            // new flow
            mapDbSpecificJar(resTopDirPath, dbName);
          }
        }
      }

      log.debug("Found resources {}", dbResEntryMap.get(dbName));
    } catch (IOException io) {
      log.error("Error getting list of dbs to load resources from", io);
      throw new LensException(io);
    } catch (Exception e) {
      log.error("Exception : ", e);
    }
  }

  public Map<String, Integer> getLatestJarFileFromDbResourceWithIndex(Path baseDir, String dbName, FileSystem serverFs,
                                               boolean shouldIncrVersion) throws LensException {
    String jarFile = null;
    Map<String, Integer> jarfileToIndexMap = new HashMap();
    try {
      int lastIndex = 0;
      Path dbFolderPath = new Path(baseDir, dbName);
      serverFs = FileSystem.newInstance(dbFolderPath.toUri(), getHiveConf());
      FileStatus[] existingFiles = serverFs.listStatus(dbFolderPath);
      for (FileStatus fs : existingFiles) {
        String fPath = fs.getPath().getName();
        String[] tokens = fPath.split("_");

        if (tokens.length > 1) {
          String lastToken = tokens[tokens.length - 1];
          int fIndex = Integer.parseInt(lastToken.substring(0, lastToken.indexOf(".jar")));
          if (fIndex > lastIndex) {
            lastIndex = fIndex;
          }
        }
      }
      if (shouldIncrVersion) {
        ++lastIndex;
      }
      jarFile =  dbName + "_" + lastIndex + ".jar";
      jarfileToIndexMap.put(jarFile, lastIndex);
      return jarfileToIndexMap;
    } catch (IOException e) {
      log.error("Error getting db specific resource for database {}", dbName, e);
      throw new LensException(e);
    }
  }

  private void mapDbSpecificJar(Path baseDir, String dbName) throws LensException {
    Map<String, Integer> jarfileToIndexMap = getLatestJarFileFromDbResourceWithIndex(baseDir, dbName, null, false);
    String latestJarFile = jarfileToIndexMap.keySet().iterator().next();
    int index = jarfileToIndexMap.values().iterator().next();
    if (index > 0) {
      Path latestJarPath = new Path(baseDir, dbName + File.separator +  latestJarFile);
      addResourceEntry(new LensSessionImpl.ResourceEntry("jar", latestJarPath.toUri().toString()), dbName);
    }
    // add common jars
    for (LensSessionImpl.ResourceEntry jar : commonResList) {
      addResourceEntry(jar, dbName);
    }
  }


  private void findResourcesInDir(FileSystem serverFs, String database, Path dbDirPath) throws IOException {
    // Check if order file is present in the directory
    List<String> jars = new ScannedPaths(dbDirPath, "jar").getFinalPaths();
    if (jars != null && !jars.isEmpty()) {
      log.info("{} picking jar in jar_order: {}", database, jars);
      for (String jar : jars) {
        if (StringUtils.isBlank(jar)) {
          // skipping empty lines. usually the last line could be empty
          continue;
        }
        Path jarFilePath = new Path(dbDirPath, jar);
        if (!jar.endsWith(".jar") || !serverFs.exists(jarFilePath)) {
          log.info("Resource skipped {} for db {}", jarFilePath, database);
          continue;
        }
        addResourceEntry(new LensSessionImpl.ResourceEntry("jar", jarFilePath.toUri().toString()), database);
      }
    } else {
      log.info("{} picking jars in file list order", database);
      for (FileStatus dbResFile : serverFs.listStatus(dbDirPath)) {
        // Skip subdirectories
        if (serverFs.isDirectory(dbResFile.getPath())) {
          continue;
        }
        String dbResName = dbResFile.getPath().getName();
        String dbResUri = dbResFile.getPath().toUri().toString();
        if (dbResName.endsWith(".jar")) {
          addResourceEntry(new LensSessionImpl.ResourceEntry("jar", dbResUri), database);
        } else {
          log.info("Resource skipped {} for db {}", dbResFile.getPath(), database);
        }
      }
    }
  }

  private void addResourceEntry(LensSessionImpl.ResourceEntry entry, String dbName) {
    log.info("Adding resource entry {} for {}", entry.getUri(), dbName);
    synchronized (dbResEntryMap) {
      List<LensSessionImpl.ResourceEntry> dbEntryList = dbResEntryMap.get(dbName);
      if (dbEntryList == null) {
        dbEntryList = new ArrayList<>();
        dbResEntryMap.put(dbName, dbEntryList);
      }
      dbEntryList.add(entry);
    }
  }

  /**
   * Load DB specific resources
   */
  private void loadMappedResources() {
    for (String db : dbResEntryMap.keySet()) {
      loadDBJars(db, dbResEntryMap.get(db));
      log.info("Loaded resources for db {} resources: {}", db, dbResEntryMap.get(db));
    }
  }

  /**
   * Add a resource to the specified database. Update class loader of the database if required.
   * @param database database name
   * @param resources resources which need to be added to the database
   */
  private synchronized void loadDBJars(String database, Collection<LensSessionImpl.ResourceEntry> resources) {
    URL[] urls = new URL[0];
    if (resources != null) {
      urls = new URL[resources.size()];
      int i = 0;
      for (LensSessionImpl.ResourceEntry res : resources) {
        try {
          urls[i++] = new URL(res.getUri());
        } catch (MalformedURLException e) {
          incrCounter(LOAD_RESOURCES_ERRORS);
          log.error("Invalid URL {} with location: {} adding to db {}", res.getUri(), res.getLocation(), database, e);
        }
      }
    }
    classLoaderCache.put(database,
      new UncloseableClassLoader(urls, getClass().getClassLoader()));
  }


  /**
   * Get class loader of a database added with database specific jars
   * @param database database
   * @return class loader from cache of classloaders for each db
   */
  protected ClassLoader getClassLoader(String database) {
    return classLoaderCache.get(database);
  }

  /**
   * Get resources added statically to the database
   * @param database db
   * @return resources added to the database, or null if no resources are noted for this database
   */
  public Collection<LensSessionImpl.ResourceEntry> getResourcesForDatabase(String database) {
    if (remoteDbResEntryMap.get(database) != null) {
      return remoteDbResEntryMap.get(database);
    } else {
      return dbResEntryMap.get(database);
    }
  }

  public void addremoteDbResourceEntry(String database, List<LensSessionImpl.ResourceEntry> resource) {
    remoteDbResEntryMap.put(database, resource);
  }

  public void addLocalDbResourceEntry(String database, List<LensSessionImpl.ResourceEntry> resource) {
    dbResEntryMap.put(database, resource);
  }

  private MetricsService getMetrics() {
    if (metricsService == null) {
      metricsService = LensServices.get().getService(MetricsService.NAME);
      if (metricsService == null) {
        throw new NullPointerException("Could not get metrics service");
      }
    }
    return metricsService;
  }

  public synchronized void addDBJar(LensSessionHandle sessionid, InputStream is) throws LensException {
    try {
      log.info("Adding jar to DB started");
      acquire(sessionid);
      String currentDB = SessionState.get().getCurrentDatabase();
      //baseDir can be local file system or HDFS
      String baseDir = getHiveConf().get(LensConfConstants.DATABASE_RESOURCE_DIR,
          LensConfConstants.DEFAULT_DATABASE_RESOURCE_DIR);
      String localDir = getHiveConf().get(LensConfConstants.DATABASE_LOCAL_RESOURCE_DIR,
          LensConfConstants.DEFAULT_LOCAL_DATABASE_RESOURCE_DIR);
      //DatabaseResourceService dbResSvc = getSession(sessionid).getDbResService();
      Path jarFilePath = null;
      if (new Path(baseDir).getFileSystem(getHiveConf()).getScheme().equals("file")) {
        // baseDir is in local filesystem, add jar to local base directory
        jarFilePath = addDBJarToLocal(is, baseDir, currentDB);
        addLocalDbResourceEntry(currentDB,
            updateDbResourceEntries(currentDB, baseDir, jarFilePath));
      } else {
        // add jar to local secondary directory first and then copy to HDFS
        jarFilePath = addDBJarToLocal(is, localDir, currentDB);
        //copy jar file to hdfs
        copyJarFileToHDFS(jarFilePath, new Path(localDir, currentDB), new Path(baseDir, currentDB));
        String jarFile = jarFilePath.toString().substring(jarFilePath.toString().lastIndexOf(File.separator) + 1);
        // Add local path resource entry
        addLocalDbResourceEntry(currentDB, updateDbResourceEntries(currentDB, localDir, jarFilePath));
        // Add remote path resource entry
        addremoteDbResourceEntry(currentDB, updateDbResourceEntries(currentDB, dbResTopDir,
                new Path(dbResTopDir, currentDB + File.separator +  jarFile)));
      }
      log.info("Adding jar to DB ended");
    } catch (IOException e) {
      log.error("Exception occoured while uploading jar file ", e);
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  private List<LensSessionImpl.ResourceEntry> updateDbResourceEntries(String currentDB,
                                                                      String baseDir, Path jarFilePath) {
    List<LensSessionImpl.ResourceEntry> resourceEntries = new ArrayList<>();
    resourceEntries.add(new LensSessionImpl.ResourceEntry("jar",
        new Path(baseDir, currentDB + jarFilePath.getName()).toUri().toString()));
    return resourceEntries;
  }

  private void copyJarFileToHDFS(Path jarFilePath, Path localPath, Path remotePath) throws LensException {
    try {
      FileSystem fs = FileSystem.newInstance(remotePath.toUri(), getHiveConf());
      if (!fs.exists(remotePath)) {
        fs.mkdirs(remotePath);
      }
      fs.copyFromLocalFile(jarFilePath, remotePath);
      log.info("Jar file Copied from {} to {} ", localPath, remotePath);
    } catch (IOException e) {
      log.error("Error while copying file to HDFS ", e);
      throw new LensException(e);
    }
  }

  private Path addDBJarToLocal(InputStream is, String baseDir, String currentDB)
    throws LensException {
    // Read list of databases in
    FileSystem serverFs = null;
    FileSystem jarOrderFs = null;
    FSDataOutputStream fos = null;

    try {
      String dbDir = baseDir + File.separator + currentDB;
      log.info("Database specific resources at {}", dbDir);
      Path resTopDirPath = new Path(dbDir);
      serverFs = FileSystem.newInstance(resTopDirPath.toUri(), getHiveConf());
      if (!serverFs.exists(resTopDirPath)) {
        // Create DB directory if its not present
        serverFs.mkdirs(resTopDirPath);
      }
      Path resJarOrderPath = new Path(dbDir, "jar_order");
      jarOrderFs = FileSystem.newInstance(resJarOrderPath.toUri(), getHiveConf());
      if (jarOrderFs.exists(resJarOrderPath)) {
        log.warn("Database jar_order file exist - {}. Database jar can't be uploaded", resJarOrderPath);
        throw new LensException("This database " + currentDB + " does not support jar upload!");
      }
      String tempFileName = currentDB + "_uploading.jar";
      Path uploadingPath = new Path(dbDir, tempFileName);
      FileSystem uploadingFs = FileSystem.newInstance(uploadingPath.toUri(), getHiveConf());
      if (uploadingFs.exists(uploadingPath)) {
        // Delete the temp file already existing, which might have created due to
        // server restart or some other unexpected reason while renaming file.
        FileUtil.fullyDelete(new File(uploadingPath.toUri().toString()));
      }
      String latestJarFile = getLatestJarFileFromDbResourceWithIndex(new Path(baseDir),
          currentDB, null, true).keySet().iterator().next();
      Path renamePath = new Path(dbDir + File.separator + latestJarFile);

      fos = serverFs.create(uploadingPath);
      IOUtils.copy(is, fos);
      fos.flush();
      if (!serverFs.rename(uploadingPath, renamePath)) {
        log.error("File rename failed from {} to {}", uploadingPath, renamePath);
        throw new LensException("File rename failed!");
      }
      return renamePath;
    } catch (IOException e) {
      log.error("Exception occoured while uploading jar file ", e);
      throw new LensException(e);
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          log.error("Error closing file system instance fos", e);
        }
      }
    }
  }

  @Override
  public HealthStatus getHealthStatus() {
    return null;
  }
}
