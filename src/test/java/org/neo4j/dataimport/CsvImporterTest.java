package org.neo4j.dataimport;

import org.apache.commons.io.FileUtils;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CsvImporterTest
{
    private File nodes;
    private File rels;
    private BatchInserter batchInserter;
    private String storePath;
    private GraphDatabaseService graphDb;

    @Before
    public void setUp() throws IOException
    {
        nodes = File.createTempFile( "nodes-import-", ".csv" );
        rels = File.createTempFile( "rels-import-", ".csv" );
        storePath = createTempDir().getAbsolutePath();
        batchInserter = new BatchInserterImpl( storePath );
    }

    private File createTempDir() throws IOException
    {
        File tempdir = File.createTempFile( "csv-import", "-store" );
        tempdir.delete();
        return tempdir;
    }

    @After
    public void tearDown()
    {
        assertTrue( "Unable to delete tempfile.", nodes.delete() );
        if ( batchInserter != null)
        {
            batchInserter.shutdown();
        }
        if (graphDb != null)
        {
            graphDb.shutdown();
        }
    }

    @Test(expected = NotFoundException.class)
    public void testEmptyImport() throws IOException
    {
        CsvImporter csvImporter = new CsvImporter( nodes, rels );
        csvImporter.importTo( batchInserter );

        importComplete();

        graphDb.getNodeById( 1 );
    }

    @Test
    public void testSingleLineImport() throws IOException
    {
        addNode( "1" );

        writeFiles();

        CsvImporter csvImporter = new CsvImporter( nodes, rels );
        csvImporter.importTo( batchInserter );

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
    public void testRelationshipImport() throws IOException
    {
        addNode( "1" );
        addNode( "2" );
        addRel( "1,2,KNOWS" );

        writeFiles();

        CsvImporter csvImporter = new CsvImporter( nodes, rels );
        csvImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        Relationship rel = node1.getSingleRelationship( DynamicRelationshipType.withName( "KNOWS" ), Direction.OUTGOING );
        assertNotNull( rel );
        Node node2 = graphDb.getNodeById( 2 );
        assertEquals( node2, rel.getEndNode() );
    }

    @Test
    public void testNodePropertyImport() throws IOException
    {
        addNode( "id,name" );
        addNode( "1,hello" );

        writeFiles();

        CsvImporter csvImporter = new CsvImporter( nodes, rels );
        csvImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        assertEquals( "hello", node1.getProperty( "name" ) );
    }

    @Test
    public void testRelationshipPropertyImport() throws IOException
    {
        addNode( "1" );
        addNode( "2" );
        addRel( "from,to,type,since@long" );
        addRel( "1,2,KNOWS,123" );

        writeFiles();

        CsvImporter csvImporter = new CsvImporter( nodes, rels );
        csvImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        Relationship rel = node1.getSingleRelationship( DynamicRelationshipType.withName( "KNOWS" ), Direction.OUTGOING );
        assertEquals( 123L, rel.getProperty( "since" ) );
    }

    @Test
    public void testPropertyTypes() throws IOException
    {

        addNode( "id,s,ss@String,l@long,i@int,sh@short,b@byte,c@char,f@float,d@double,bo@boolean" );
        addNode( "1,hello,foo,9999999999999999,888888888,777,66,g,0.2345,0.1234,true" );
        addNode( "2" );
        addRel( "from,to,type,s,ss@String,l@long,i@int,sh@short,b@byte,c@char,f@float,d@double,bo@boolean" );
        addRel( "1,2,KNOWS,hello,foo,9999999999999999,888888888,777,66,g,0.2345,0.1234,true" );

        writeFiles();

        CsvImporter csvImporter = new CsvImporter( nodes, rels );
        csvImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        assertEquals( "hello", node1.getProperty( "s" ) );
        assertEquals( "foo", node1.getProperty( "ss" ) );
        assertEquals( 9999999999999999L, node1.getProperty( "l" ) );
        assertEquals( 888888888, node1.getProperty( "i" ) );
        assertEquals( (short)777, node1.getProperty( "sh" ) );
        assertEquals( (byte)66, node1.getProperty( "b" ) );
        assertEquals( 'g', node1.getProperty( "c" ) );
        assertEquals( 0.2345f, node1.getProperty( "f" ) );
        assertEquals( 0.1234d, node1.getProperty( "d" ) );
        assertEquals( true, node1.getProperty( "bo" ) );

        Relationship rel = node1.getSingleRelationship( DynamicRelationshipType.withName( "KNOWS" ), Direction.OUTGOING );
        assertEquals( "hello", rel.getProperty( "s" ) );
        assertEquals( "foo", rel.getProperty( "ss" ) );
        assertEquals( 9999999999999999L, rel.getProperty( "l" ) );
        assertEquals( 888888888, rel.getProperty( "i" ) );
        assertEquals( (short)777, rel.getProperty( "sh" ) );
        assertEquals( (byte)66, rel.getProperty( "b" ) );
        assertEquals( 'g', rel.getProperty( "c" ) );
        assertEquals( 0.2345f, rel.getProperty( "f" ) );
        assertEquals( 0.1234d, rel.getProperty( "d" ) );
        assertEquals( true, rel.getProperty( "bo" ) );
    }

    @Test
    public void testSparseProperties() throws IOException
    {
        addNode( "id,name,age@long" );
        addNode( "1,a" );
        addNode( "2,,25" );
        addNode( "3,c,26" );

        writeFiles();

        CsvImporter csvImporter = new CsvImporter( nodes, rels );
        csvImporter.importTo( batchInserter );

        importComplete();

        Node node1 = graphDb.getNodeById( 1 );
        assertEquals( "a", node1.getProperty( "name" ) );
        assertEquals( false, node1.hasProperty( "age" ) );
        Node node2 = graphDb.getNodeById( 2 );
        assertEquals( false, node2.hasProperty( "name" ) );
        assertEquals( 25L, node2.getProperty( "age" ) );
        Node node3 = graphDb.getNodeById( 3 );
        assertEquals( "c", node3.getProperty( "name" ) );
        assertEquals( 26L, node3.getProperty( "age" ) );
    }

    private void importComplete()
    {
        batchInserter.shutdown();
        batchInserter = null;
        graphDb = new EmbeddedGraphDatabase( storePath );
    }

    List<String> nodeLines = new ArrayList<String>();
    List<String> relLines = new ArrayList<String>();

    private void addNode( String line ) throws IOException
    {
        nodeLines.add( line );
    }

    private void addRel( String line ) throws IOException
    {
        relLines.add( line );
    }

    private void writeFiles() throws IOException
    {
        FileUtils.writeLines( nodes, nodeLines );
        FileUtils.writeLines( rels, relLines );
    }
}
