package org.neo4j.dataimport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class JdbcImporterTest
{
    private static int dbId = 1;

    private Connection connection;
    private BatchInserter batchInserter;
    private String storePath;
    private GraphDatabaseService graphDb;

    @Before
    public void setUp() throws IOException
    {
        storePath = createTempDir().getAbsolutePath();
        batchInserter = new BatchInserterImpl( storePath );
    }

    private File createTempDir() throws IOException
    {
        File tempdir = File.createTempFile( "csv-import", "-store" );
        tempdir.delete();
        return tempdir;
    }

    @Before
    public void initJdbc() throws Exception
    {
        try
        {
            Class.forName( "org.hsqldb.jdbcDriver" );
        }
        catch ( Exception e )
        {
            System.out.println( "ERROR: failed to load HSQLDB JDBC driver." );
            e.printStackTrace();
            return;
        }
        connection = DriverManager.getConnection( "jdbc:hsqldb:mem:import" + dbId++, "sa", "" );
    }

    @After
    public void tearDownDbs()
    {
        if ( batchInserter != null )
        {
            batchInserter.shutdown();
        }
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
    }

    @After
    public void closeJdbc() throws Exception
    {
        connection.close();
    }

    private void importComplete()
    {
        batchInserter.shutdown();
        batchInserter = null;
        graphDb = new EmbeddedGraphDatabase( storePath );
    }

    @Test(expected = NotFoundException.class)
    public void testEmptyImport() throws SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY)" );
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR)" );

        JdbcImporter jdbcImporter = new JdbcImporter(connection, "nodes", "rels");
        jdbcImporter.importTo( batchInserter );

        importComplete();

        graphDb.getNodeById( 1 );
    }

    @Test
    public void testImportSingleNode() throws SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY)" );
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR)" );
        update( "INSERT INTO nodes (id) VALUES(1)" );

        JdbcImporter jdbcImporter = new JdbcImporter(connection, "nodes", "rels");
        jdbcImporter.importTo( batchInserter );

        importComplete();

        assertNotNull( graphDb.getNodeById( 1 ) );
        try
        {
            graphDb.getNodeById( 2 );
            fail( "Should have thrown an exception." );
        }
        catch ( Exception e )
        {
        }
    }

    @Test
    public void testImportRelationship() throws SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY)" );
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR)" );
        update( "INSERT INTO nodes (id) VALUES(1)" );
        update( "INSERT INTO nodes (id) VALUES(2)" );
        update( "INSERT INTO rels (src,dest,type) VALUES(1,2,'KNOWS')" );

        JdbcImporter jdbcImporter = new JdbcImporter(connection, "nodes", "rels");
        jdbcImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        Relationship rel = node1.getSingleRelationship( DynamicRelationshipType.withName( "KNOWS" ), Direction.OUTGOING );
        assertNotNull( "Relationship did not exist.", rel );
        Node node2 = graphDb.getNodeById( 2 );
        assertEquals( node2, rel.getEndNode() );
    }

    @Test
    public void testNodePropertyImport() throws SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY, name VARCHAR)" );
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR)" );
        update( "INSERT INTO nodes (id,name) VALUES(1,'hello')" );

        JdbcImporter jdbcImporter = new JdbcImporter(connection, "nodes", "rels");
        jdbcImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        assertEquals( "hello", node1.getProperty( "name" ) );
    }

    @Test
    public void testRelationshipPropertyImport() throws SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY)" );
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR, since BIGINT)" );
        update( "INSERT INTO nodes (id) VALUES(1)" );
        update( "INSERT INTO nodes (id) VALUES(2)" );
        update( "INSERT INTO rels (src,dest,type,since) VALUES(1,2,'KNOWS', 123)" );

        JdbcImporter jdbcImporter = new JdbcImporter(connection, "nodes", "rels");
        jdbcImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        Relationship rel = node1.getSingleRelationship( DynamicRelationshipType.withName( "KNOWS" ), Direction.OUTGOING );
        assertEquals( 123L, rel.getProperty( "since" ) );
    }

    @Test
    public void testPropertyTypes() throws IOException, SQLException
    {

        update( "CREATE TABLE nodes (id BIGINT IDENTITY, s VARCHAR, l BIGINT, i INTEGER, sh SMALLINT, b TINYINT, f FLOAT, d DOUBLE, bo BOOLEAN)" );
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR, s VARCHAR, l BIGINT, i INTEGER, sh SMALLINT, b TINYINT, f FLOAT, d DOUBLE, bo BOOLEAN)" );
        update( "INSERT INTO nodes (id,s,l,i,sh,b,f,d,bo) VALUES(1,'hello',9999999999999999,888888888,777,66,0.2345,0.1234,TRUE)" );
        update( "INSERT INTO nodes (id) VALUES(2)" );
        update( "INSERT INTO rels (src,dest,type,s,l,i,sh,b,f,d,bo) VALUES(1,2,'KNOWS','hello',9999999999999999,888888888,777,66,0.2345,0.1234,TRUE)" );

        JdbcImporter jdbcImporter = new JdbcImporter(connection, "nodes", "rels");
        jdbcImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        assertEquals( "hello", node1.getProperty( "s" ) );
        assertEquals( 9999999999999999L, node1.getProperty( "l" ) );
        assertEquals( 888888888, node1.getProperty( "i" ) );
        assertEquals( (short)777, node1.getProperty( "sh" ) );
        assertEquals( (byte)66, node1.getProperty( "b" ) );
        assertEquals( 0.2345f, node1.getProperty( "f" ) );
        assertEquals( 0.1234d, node1.getProperty( "d" ) );
        assertEquals( true, node1.getProperty( "bo" ) );

        Relationship rel = node1.getSingleRelationship( DynamicRelationshipType.withName( "KNOWS" ), Direction.OUTGOING );
        assertEquals( "hello", rel.getProperty( "s" ) );
        assertEquals( 9999999999999999L, rel.getProperty( "l" ) );
        assertEquals( 888888888, rel.getProperty( "i" ) );
        assertEquals( (short)777, rel.getProperty( "sh" ) );
        assertEquals( (byte)66, rel.getProperty( "b" ) );
        assertEquals( 0.2345f, rel.getProperty( "f" ) );
        assertEquals( 0.1234d, rel.getProperty( "d" ) );
        assertEquals( true, rel.getProperty( "bo" ) );
    }

    @Test
    public void testSparseProperties() throws IOException, SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY, name VARCHAR, age BIGINT)" );
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR)" );
        update( "INSERT INTO nodes (id,name) VALUES(1,'a')" );
        update( "INSERT INTO nodes (id,age) VALUES(2,25)" );
        update( "INSERT INTO nodes (id,name,age) VALUES(3,'c',26)" );

        JdbcImporter jdbcImporter = new JdbcImporter(connection, "nodes", "rels");
        jdbcImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        assertEquals( "a", node1.getProperty( "name" ) );
        assertEquals( 0L, node1.getProperty( "age" ) );
        Node node2 = graphDb.getNodeById( 2 );
        assertEquals( false, node2.hasProperty( "name" ) );
        assertEquals( 25L, node2.getProperty( "age" ) );
        Node node3 = graphDb.getNodeById( 3 );
        assertEquals( "c", node3.getProperty( "name" ) );
        assertEquals( 26L, node3.getProperty( "age" ) );
    }

    private void update( String sql ) throws SQLException
    {
        Statement statement = connection.createStatement();
        statement.executeUpdate( sql );
        statement.close();
    }

    private ResultSet query( String sql ) throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( sql );
        statement.close();
        return resultSet;
    }
}
