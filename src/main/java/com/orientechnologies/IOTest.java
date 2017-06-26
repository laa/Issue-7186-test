package com.orientechnologies;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IOTest {
  private static final String DB_NAME = "iotest";
  private static OrientGraphFactory graphFactory;

  private static final ExecutorService executorService = Executors.newCachedThreadPool();
  private static final AtomicInteger   idGen           = new AtomicInteger();

  private static List<Integer> recordIds;

  public static void main(String[] args) {
    try {
      init();

      phase1();
      phase2();

      final int cycles = 10;
      System.out.println("Repeat phases 3-5 in cycle, " + cycles + " iterations");

      for (int i = 0; i < cycles; i++) {
        System.out.println("Iteration " + i + " of " + cycles + " for phases 3-5 cycle is started");

        phase3();
        phase4();
        phase5();
      }

      System.out.println("Phases 3-5 cycle is completed");

      OrientGraph graph = graphFactory.getTx();

      System.out.println("Final: Vertexes in DB: " + graph.countVertices());
      System.out.println("Final: Edges in DB: " + graph.countEdges());

      graph.shutdown();

      executorService.shutdown();

      System.out.println("Execution is completed");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void init() {
    System.out.println("Start initialization");

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + DB_NAME);

    if (db.exists()) {
      System.out.println("Database " + DB_NAME + " already exists and will be dropped");

      db.open("admin", "admin");
      db.drop();

      System.out.println("Database " + DB_NAME + " is dropped");
    }

    db.create();
    db.close();

    System.out.println("New database " + DB_NAME + " is created");

    graphFactory = new OrientGraphFactory("plocal:" + DB_NAME);

    OrientGraphNoTx noTXGraph = graphFactory.getNoTx();

    OrientVertexType baseVertexType = noTXGraph.createVertexType("base_vertex");
    baseVertexType.createProperty("pid", OType.INTEGER);

    baseVertexType.createIndex("id_index", OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX, "pid");

    System.out.println("Base vertex type and index were created");

    for (int i = 0; i < 104; i++) {
      noTXGraph.createVertexType("vertex_" + i, baseVertexType);
    }

    System.out.println("104 vertex types were created");

    for (int i = 0; i < 104; i++) {
      noTXGraph.createEdgeType("edge_" + i);
    }

    System.out.println("104 edge types were created");

    noTXGraph.shutdown();

    reopenStorage();

    System.out.println("Initialization is completed");
  }

  private static void reopenStorage() {
    graphFactory.close();
    System.out.println("GrpahFactory was closed");

    Orient.instance().shutdown();
    System.out.println("Orient instance was shut down");

    Orient.instance().startup();
    Orient.instance().removeShutdownHook();

    System.out.println("Orient instance was started");

    graphFactory = new OrientGraphFactory("plocal:" + DB_NAME);
    System.out.println("New GraphFactory instance was created");
  }

  private static void phase1() throws Exception {
    System.out.println("Phase 1 is started");

    final int batches = 100;
    final int batchIterations = 1000;
    final CountDownLatch latch = new CountDownLatch(1);

    recordIds = new ArrayList<Integer>();

    for (int i = 0; i < batches; i++) {
      System.out.println("Batch " + i + " of " + batches + " is started");

      final List<Future<List<Integer>>> futures = new ArrayList<Future<List<Integer>>>();

      for (int n = 0; n < 8; n++) {
        final String[] vertexTypes = new String[13];

        for (int k = n * 13; k < (n + 1) * 13; k++) {
          vertexTypes[k - n * 13] = "vertex_" + k;
        }

        futures.add(executorService.submit(new VertexAdder(vertexTypes, batchIterations, latch)));
      }

      latch.countDown();
      System.out.println("Executors which add vertexes are started");

      for (Future<List<Integer>> future : futures) {
        recordIds.addAll(future.get());
      }

      System.out.println("Executors which add vertexes are finished, " + recordIds.size() + " vertexes currently in database.");

      reopenStorage();

      System.out.println("Batch " + i + " of " + batches + " is completed");
    }

    System.out.println("Phase 1 is completed");
  }

  private static void phase2() throws Exception {
    System.out.println("Phase 2 is started");

    final int batches = 100;
    final int batchIterations = 10000;

    int counter = 0;

    for (int i = 0; i < batches; i++) {
      System.out.println("Batch " + i + " of " + batches + " is started");

      final CountDownLatch latch = new CountDownLatch(1);

      final List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
      for (int n = 0; n < 8; n++) {
        final String[] edgeTypes = new String[13];

        for (int k = n * 13; k < (n + 1) * 13; k++) {
          edgeTypes[k - n * 13] = "edge_" + k;
        }

        futures.add(executorService.submit(new EdgeAdder(edgeTypes, batchIterations, latch)));
      }

      latch.countDown();
      System.out.println("Executors which add edges are started");

      for (Future<Integer> future : futures) {
        counter += future.get();
      }

      System.out.println("Executors which add edges are finished, " + counter + " edges are added.");

      reopenStorage();

      System.out.println("Batch " + i + " of " + batches + " is completed");
    }

    System.out.println("Phase 2 is completed");
  }

  private static void phase3() throws Exception {
    System.out.println("Phase 3 is started");

    final int batches = 50;
    final int batchIterations = 13000;

    Set<Integer> rids = new HashSet<Integer>(recordIds);
    int counter = 0;

    for (int i = 0; i < batches; i++) {
      System.out.println("Batch " + i + " of " + batches + " is started");

      final CountDownLatch latch = new CountDownLatch(1);
      final List<Future<List<Integer>>> futures = new ArrayList<Future<List<Integer>>>();

      for (int n = 0; n < 8; n++) {
        futures.add(executorService.submit(new VertexDeleter(n, batchIterations, latch)));
      }

      latch.countDown();
      System.out.println("Executors which delete vertexes are started");

      for (Future<List<Integer>> future : futures) {
        List<Integer> removedRids = future.get();
        counter += removedRids.size();

        rids.removeAll(removedRids);
      }

      System.out.println("Executors which delete vertexes are finished, " + counter + " vertexes are deleted.");

      reopenStorage();

      System.out.println("Batch " + i + " of " + batches + " is completed");
    }

    recordIds = new ArrayList<Integer>(rids);

    System.out.println("Phase 3 is finished, " + recordIds.size() + " vertexes in database");
  }

  private static void phase4() throws Exception {
    System.out.println("Phase 4 is started");

    final int batches = 50;
    final int batchIterations = 1000;
    final CountDownLatch latch = new CountDownLatch(1);

    for (int i = 0; i < batches; i++) {
      System.out.println("Batch " + i + " of " + batches + " is started");

      final List<Future<List<Integer>>> futures = new ArrayList<Future<List<Integer>>>();

      for (int n = 0; n < 8; n++) {
        final String[] vertexTypes = new String[13];

        for (int k = n * 13; k < (n + 1) * 13; k++) {
          vertexTypes[k - n * 13] = "vertex_" + k;
        }

        futures.add(executorService.submit(new VertexAdder(vertexTypes, batchIterations, latch)));
      }

      latch.countDown();
      System.out.println("Executors which add vertexes are started");

      for (Future<List<Integer>> future : futures) {
        recordIds.addAll(future.get());
      }

      System.out.println("Executors which add vertexes are finished, " + recordIds.size() + " vertexes currently in database.");

      reopenStorage();

      System.out.println("Batch " + i + " of " + batches + " is completed");
    }

    System.out.println("Phase 4 is completed");

  }

  private static void phase5() throws Exception {
    System.out.println("Phase 5 is started");

    final int batches = 50;
    final int batchIterations = 10000;

    int counter = 0;

    for (int i = 0; i < batches; i++) {
      System.out.println("Batch " + i + " of " + batches + " is started");

      final CountDownLatch latch = new CountDownLatch(1);

      final List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
      for (int n = 0; n < 8; n++) {
        final String[] edgeTypes = new String[13];

        for (int k = n * 13; k < (n + 1) * 13; k++) {
          edgeTypes[k - n * 13] = "edge_" + k;
        }

        futures.add(executorService.submit(new EdgeAdder(edgeTypes, batchIterations, latch)));
      }

      latch.countDown();
      System.out.println("Executors which add edges are started");

      for (Future<Integer> future : futures) {
        counter += future.get();
      }

      System.out.println("Executors which add edges are finished, " + counter + " edges are added.");

      reopenStorage();

      System.out.println("Batch " + i + " of " + batches + " is completed");
    }

    System.out.println("Phase 5 is completed");
  }

  private static final class VertexAdder implements Callable<List<Integer>> {
    private final String[]       vertexTypes;
    private final int            batchIterations;
    private final CountDownLatch latch;

    private final List<Integer> rids = new ArrayList<Integer>();

    public VertexAdder(String[] vertexTypes, int batchIterations, CountDownLatch latch) {
      this.vertexTypes = vertexTypes;
      this.batchIterations = batchIterations;
      this.latch = latch;
    }

    public List<Integer> call() throws Exception {
      latch.await();

      System.out.println("Adder for " + Arrays.toString(vertexTypes) + " vertex types is started");
      OrientGraph graph = graphFactory.getTx();

      for (int i = 0; i < batchIterations; i++) {
        for (String cl : vertexTypes) {
          final OrientVertex vertex = graph.addVertex("class:" + cl);

          final int id = idGen.getAndIncrement();
          vertex.setProperty("pid", id);

          graph.commit();

          rids.add(id);
        }

        if (rids.size() % 10000 == 0) {
          System.out.println(rids.size() + " vertexes were added for types " + Arrays.toString(vertexTypes));
        }
      }

      graph.shutdown();

      System.out.println("Executor for " + Arrays.toString(vertexTypes) + " vertex types is finished");
      return rids;
    }
  }

  private static final class EdgeAdder implements Callable<Integer> {
    private static final int VERTEXES_IN_BATCH = 100;

    private final String[]       edgeTypes;
    private final int            batchIterations;
    private final CountDownLatch latch;

    private EdgeAdder(String[] edgeTypes, int batchIterations, CountDownLatch latch) {
      this.edgeTypes = edgeTypes;
      this.batchIterations = batchIterations;
      this.latch = latch;
    }

    public Integer call() throws Exception {
      int counter = 0;

      Random random = new Random();

      latch.await();

      System.out.println("Adder for " + Arrays.toString(edgeTypes) + " edge types is started");

      OrientGraph graph = graphFactory.getTx();

      for (int i = 0; i < batchIterations; i++) {
        for (String cl : edgeTypes) {
          while (true)
            try {
              int fromIdIndex = random.nextInt(recordIds.size());
              Integer fromId = recordIds.get(fromIdIndex);

              Iterator<Vertex> fromIterator = graph.getVertices("base_vertex.pid", fromId).iterator();
              OrientVertex fromVertex = (OrientVertex) fromIterator.next();

              for (int n = 0; n < VERTEXES_IN_BATCH; n++) {
                int toIdIndex = random.nextInt(recordIds.size());
                Integer toId = recordIds.get(toIdIndex);

                Iterator<Vertex> toIterator = graph.getVertices("base_vertex.pid", toId).iterator();
                OrientVertex toVertex = (OrientVertex) toIterator.next();

                graph.addEdge("class:" + cl, fromVertex, toVertex, cl);
              }

              graph.commit();

              counter += VERTEXES_IN_BATCH;

              if (counter % 10000 == 0) {
                System.out.println(counter + " edges were added for types " + Arrays.toString(edgeTypes));
              }
              break;
            } catch (ONeedRetryException e) {
              continue;
            }
        }
      }

      graph.shutdown();

      System.out.println("Adder for " + Arrays.toString(edgeTypes) + " edge types is finished");

      return counter;
    }
  }

  private static final class VertexDeleter implements Callable<List<Integer>> {
    private final int            id;
    private final int            batchIterations;
    private final CountDownLatch latch;

    private final List<Integer> deletedRids = new ArrayList<Integer>();

    private VertexDeleter(int id, int batchIterations, CountDownLatch latch) {
      this.id = id;
      this.batchIterations = batchIterations;
      this.latch = latch;
    }

    public List<Integer> call() throws Exception {
      Random random = new Random();

      latch.await();

      System.out.println("Vertex deleter with id = " + id + " is started");

      OrientGraph graph = graphFactory.getTx();

      for (int i = 0; i < batchIterations; i++) {
        while (true) {
          try {
            int index = random.nextInt(recordIds.size());
            Integer pid = recordIds.get(index);

            Iterator<Vertex> fromIterator = graph.getVertices("base_vertex.pid", pid).iterator();
            if (!fromIterator.hasNext())
              continue;

            OrientVertex vertex = (OrientVertex) fromIterator.next();
            if (vertex == null)
              continue;

            vertex.remove();
            graph.commit();

            deletedRids.add(pid);

            if (deletedRids.size() % 10000 == 0) {
              System.out.println(deletedRids.size() + " vertexes were removed in executor with id = " + this.id);
            }

            break;
          } catch (ONeedRetryException | ORecordNotFoundException e) {
            continue;
          }
        }
      }

      graph.shutdown();

      System.out.println("Vertex deleter with id = " + id + " is finished");

      return deletedRids;
    }
  }
}


