package com.hybris.titan.dynamodb.issue;

import com.hybris.titan.TitanConfigurationProvider;

import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/META-INF/applicationContext.xml"})
public class TitanDynamoDbIssueTest
{

	private static final Logger LOG = LoggerFactory.getLogger(TitanDynamoDbIssueTest.class);

	private final static String TENANT = "tenant";
	private final static String ID = "id";
	private final static String CATEGORY = "category";

	private final static String TEST_TENANT = "test_tenant";

	private static final int THREAD_COUNT = 8;
	private static final int TASK_COUNT = 16;
	private static final int VERTEX_COUNT = 10;
	private static final int ITERATION_COUNT = 5;

	private final Random RANDOM = new Random(System.currentTimeMillis());

	@Autowired
	private TitanConfigurationProvider titanConfigurationProvider;

	private StandardTitanGraph graph;
	private VertexLabel categoryLabel;
	private PropertyKey tenantKey;

	@Before
	public void setup() throws FileNotFoundException, ConfigurationException
	{
		LOG.debug("setup()");

		GraphDatabaseConfiguration conf = this.titanConfigurationProvider.load();
		this.graph = new StandardTitanGraph(conf);

		LOG.debug("openManagement()");
		final TitanManagement mgmt = this.graph.openManagement();

		try {
			LOG.debug("create label");
			this.categoryLabel = mgmt.makeVertexLabel(CATEGORY).make();
			LOG.debug("create key");
			this.tenantKey = mgmt.makePropertyKey(TENANT).dataType(String.class).make();
			LOG.debug("create index");
			mgmt.buildIndex("tenantForCategoryIndex", Vertex.class).addKey(this.tenantKey).indexOnly(this.categoryLabel).buildCompositeIndex();
			mgmt.commit();
			LOG.debug("index created");
		} catch(SchemaViolationException e) {
			LOG.debug("got exception ", e.getMessage());
			LOG.debug("getting label and key");
			this.categoryLabel = mgmt.getVertexLabel(CATEGORY);
			this.tenantKey = mgmt.getPropertyKey(TENANT);
			LOG.debug("got label and key");
		}

		cleanAll();

	}

	@After
	public void close() {
		this.graph.close();
	}

	@Test
	public void testSingle() {
		this.createTestTask().run();
	}

	@Test
	public void testParallel() {
		ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);

		List<Callable<Object>> tasks = new LinkedList<>();


		for(int i = 0; i < TASK_COUNT; i++) {
			tasks.add(Executors.callable(this.createTestTask()));
		}

		try
		{
			es.invokeAll(tasks);
		}
		catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}

		es.shutdown();

		validate();
	}

	private Runnable createTestTask() {
		return () -> {
			for(int i = 0; i < ITERATION_COUNT; i++)
			{
				sleep();
				LOG.debug("performing parallel task - start");
				create();
				//validate();
				update();
				//validate();
				//ścleanUsingIndex();
				LOG.debug("performing parallel task - done");
			}
		};
	}

	private void create()
	{
		LOG.debug("create() - start");
		executeUpdate(g -> {
			for(int i = 0; i < VERTEX_COUNT; i++)
			{
				final TitanVertex vertex = g.addVertex(CATEGORY);
				vertex.property(TENANT, TEST_TENANT);
				vertex.property(ID, UUID.randomUUID().toString());
			}
		});
		LOG.debug("create() - end");
	}

	private void update()
	{
		LOG.debug("update() - start");
		executeUpdate(g -> {
			g.traversal().V().has(CATEGORY, TENANT, TEST_TENANT).toSet().forEach(v -> v.property("updated", UUID.randomUUID().toString()));
		});
		LOG.debug("update() - end");
	}

	private void cleanAll() {
		LOG.debug("cleanAll() - start");
		executeUpdate(g -> {
			g.traversal().V().drop().iterate();
		});
		LOG.debug("cleanAll() - end");
	}

	private void cleanUsingIndex() {
		LOG.debug("cleanUsingIndex() - start");
		executeBatchUpdate(g -> {
			g.traversal().V().has(CATEGORY, TENANT, TEST_TENANT).toSet().forEach(e -> e.remove());
		});
		LOG.debug("cleanUsingIndex() - end");
	}

	private void validate() {
		executeRead(g -> {
			final Long noIndexCount = g.traversal().V().count().next();
			final Long indexCount = g.traversal().V().has(CATEGORY, TENANT, TEST_TENANT).count().next();

			Assert.assertEquals(noIndexCount, indexCount);
		});
	}

	private void sleep() {
		try
		{
			long time = RANDOM.nextInt(1000);
			LOG.debug("sleep(" + time + ")");
			Thread.sleep(time);
		}
		catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}
	}

	private void executeUpdate(final Consumer<TitanTransaction> c) {
		this.executeUpdate(c, false);
	}

	private void executeBatchUpdate(final Consumer<TitanTransaction> c) {
		this.executeUpdate(c, true);
	}


	private void executeUpdate(final Consumer<TitanTransaction> c, boolean batchEnabled) {
		TransactionBuilder builder = this.graph.buildTransaction();


		if (batchEnabled) {
			builder = builder.enableBatchLoading().dirtyVertexSize(1000);
		}

		final TitanTransaction tx = builder.start();

		try {
			LOG.debug("executeUpdate(" + batchEnabled + ") - start");
			c.accept(tx);
			LOG.debug("commit()");
			tx.commit();
		} catch(Exception e) {
			LOG.debug("rollback()");
			tx.rollback();
			throw e;
		}

		LOG.debug("executeUpdate(" + batchEnabled + ") - done");
	}

	private void executeRead(final Consumer<TitanTransaction> c) {
		final TitanTransaction tx = this.graph.buildTransaction().readOnly().start();

		try {
			LOG.debug("executeRead() - start");
			c.accept(tx);
			LOG.debug("rollback()");
			tx.rollback();
		} catch(Exception e) {
			LOG.debug("rollback()");
			tx.rollback();
			throw e;
		}

		LOG.debug("executeRead() - done");
	}

}
