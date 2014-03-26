package com.inmobi.grill.examples;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.metastore.DimensionTable;
import com.inmobi.grill.api.metastore.FactTable;
import com.inmobi.grill.api.metastore.ObjectFactory;
import com.inmobi.grill.api.metastore.XCube;
import com.inmobi.grill.api.metastore.XStorage;
import com.inmobi.grill.api.metastore.XStorageTables;
import com.inmobi.grill.client.GrillConnection;
import com.inmobi.grill.client.GrillConnectionParams;
import com.inmobi.grill.client.GrillMetadataClient;

public class SampleMetastore {
  private GrillConnection connection;
  private GrillMetadataClient metaClient;
  private JAXBContext jaxbContext;
  private Unmarshaller jaxbUnmarshaller;
  private APIResult result;
  private int retCode = 0;

  public SampleMetastore() throws JAXBException {
    GrillConnectionParams params = new GrillConnectionParams();
    connection = new GrillConnection(params);
    connection.open();
    
    metaClient = new GrillMetadataClient(connection);
    jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
    jaxbUnmarshaller = jaxbContext.createUnmarshaller();
  }

  public void close() {
    connection.close();
  }

  public void createCube() throws JAXBException, IOException {
    XCube cube = (XCube)readFromXML("sample-cube.xml");
    if (cube != null) {
      result = metaClient.createCube(cube);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating cube from:sample-cube.xml failed");
        retCode = 1;
      }
    }
  }

  private Object readFromXML(String filename) throws JAXBException, IOException {
    InputStream file = getClass().getClassLoader().getResourceAsStream(filename);
    if (file == null) {
      throw new IOException("File not found:" + filename);
     }
    return ((JAXBElement)jaxbUnmarshaller.unmarshal(file)).getValue();
  }

  private void createStorage(String fileName)
      throws JAXBException, IOException {
    XStorage local = (XStorage)readFromXML(fileName);
    if (local != null) {
      result = metaClient.createNewStorage(local);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating storage from:" + fileName + " failed");
        retCode = 1;
      }
    }
  }
  public void createStorages() throws JAXBException, IOException {
    createStorage("local-storage.xml");
    createStorage("local-cluster-storage.xml");
    createStorage("db-storage.xml");
  }

  public void createAll() throws JAXBException, IOException {
    createStorages();
    createCube();
    createFacts();
    createDimensions();
  }

  private void createDimensions() throws JAXBException, IOException {
    DimensionTable dim = (DimensionTable)readFromXML("dim_table.xml");
    XStorageTables storageTables = (XStorageTables)readFromXML("dim1-storage-tables.xml");
    if (dim != null && storageTables != null) {
      result = metaClient.createDimensionTable(dim, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dim table from: dim_table.xml and dim1-storage-tables.xml failed");
        retCode = 1;
      }
    }
    dim = (DimensionTable)readFromXML("dim_table2.xml");
    storageTables = (XStorageTables)readFromXML("dim2-storage-tables.xml");
    if (dim != null && storageTables != null) {
      result = metaClient.createDimensionTable(dim, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating dim table from: dim_table2.xml and dim2-storage-tables.xml failed");
        retCode = 1;
      }
    }
  }

  private void createFacts() throws JAXBException, IOException {
    FactTable fact = (FactTable)readFromXML("fact1.xml");
    XStorageTables storageTables = (XStorageTables)readFromXML("fact1-storage-tables.xml");
    if (fact != null && storageTables != null) {
      result = metaClient.createFactTable(fact, storageTables);
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating fact table from: fact1.xml and fact1-storage-tables.xml failed");
        retCode = 1;
      }
    } 
    fact = (FactTable)readFromXML("fact2.xml");
    storageTables = (XStorageTables)readFromXML("fact2-storage-tables.xml");
    if (fact != null && storageTables != null) {
      result = metaClient.createFactTable(fact, new XStorageTables());
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating fact table from: fact2.xml and fact2-storage-tables.xml failed");
        retCode = 1;
      }
    }
    fact = (FactTable)readFromXML("rawfact.xml");
    storageTables = (XStorageTables)readFromXML("rawfact-storage-tables.xml");
    if (fact != null && storageTables != null) {
      result = metaClient.createFactTable(fact, new XStorageTables());
      if (result.getStatus().equals(APIResult.Status.FAILED)) {
        System.out.println("Creating fact table from: rawfact.xml and rawfact-storage-tables.xml failed");
        retCode = 1;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    SampleMetastore metastore = null;
    try {
     metastore = new SampleMetastore();
     if (args.length > 0) {
       if (args[0].equals("-db")) {
         String dbName = args[1];
         metastore.metaClient.createDatabase(dbName, true);
         metastore.metaClient.setDatabase(dbName);
       }
     }
    metastore.createAll();
    System.out.println("Created sample metastore!");
    System.out.println("Database:" + metastore.metaClient.getCurrentDatabase());;
    System.out.println("Storages:" + metastore.metaClient.getAllStorages());;
    System.out.println("Cubes:" + metastore.metaClient.getAllCubes());;
    System.out.println("Fact tables:" + metastore.metaClient.getAllFactTables());;
    System.out.println("Dimension tables:" + metastore.metaClient.getAllDimensionTables());
    if (metastore.retCode != 0) {
      System.exit(metastore.retCode);
    }
    } finally {
      if (metastore != null) {
        metastore.close();
      }
    }
  }
}