package org.neo4j.dataimport;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class CsvImporter implements BatchInserterImporter
{
    private File nodes;
    private File rels;

    public CsvImporter( File nodes, File rels )
    {
        this.nodes = nodes;
        this.rels = rels;
    }

    @Override
    public void importTo( BatchInserter target )
    {
        try
        {
            importNodes( target );
            importRels( target );
        }
        catch ( Exception e )
        {
            throw new DataImportException( e );
        }
    }

    public static void main( String[] args )
    {
        if ( args.length != 3 )
        {
            System.out.println( "Args: <target store dir> <nodes CSV> <relationships CSV>" );
            System.exit( 1 );
        }
        String storeDir = args[0];
        BatchInserter batchInserter = new BatchInserterImpl( storeDir, getConfig( storeDir ) );
        try
        {
            new CsvImporter( new File( args[1] ), new File( args[2] ) ).importTo( batchInserter );
        }
        finally
        {
            batchInserter.shutdown();
        }
    }

    private static Map<String, String> getConfig( String storeDir )
    {
        File configFile = new File( storeDir, "neo4j.properties" );
        if ( configFile.exists() )
        {
            return BatchInserterImpl.loadProperties( configFile.getAbsolutePath() );
        }
        return new HashMap<String, String>();
    }

    private void importNodes( BatchInserter target ) throws FileNotFoundException
    {
        List<Pair<String, String>> nodePropertyKeys = null;
        long counter = 0;
        Scanner nodeScanner = new Scanner( nodes );
        while ( nodeScanner.hasNextLine() )
        {
            String[] nodeParts = nodeScanner.nextLine().split( "," );
            List<String> properties = Arrays.asList( nodeParts ).subList( 1, nodeParts.length );
            long id;
            try
            {
                id = Long.parseLong( nodeParts[0] );
            }
            catch ( NumberFormatException e )
            {
                if ( nodePropertyKeys != null )
                {
                    throw new IllegalStateException( "Can only set property keys once." );
                }
                if ( properties.size() > 0 )
                {
                    nodePropertyKeys = parsePropertyKeys( properties );
                }
                continue;
            }
            target.createNode( id, getProperties( properties, nodePropertyKeys ) );
            if ( ++counter % 100000 == 0 ) System.out.println( "Created " + counter + " nodes." );
        }
    }

    private void importRels( BatchInserter target ) throws FileNotFoundException
    {
        List<Pair<String, String>> relPropertyKeys = null;
        long counter = 0;
        Scanner nodeScanner = new Scanner( rels );
        while ( nodeScanner.hasNextLine() )
        {
            String[] relParts = nodeScanner.nextLine().split( "," );
            if ( relParts.length < 3 )
            {
                throw new IllegalStateException( "Relationship must have at least <from>,<to>,<type>" );
            }
            List<String> properties = Arrays.asList( relParts ).subList( 3, relParts.length );
            System.out.println( properties );
            long from;
            long to;
            RelationshipType type;
            try
            {
                from = Long.parseLong( relParts[0] );
                to = Long.parseLong( relParts[1] );
                type = DynamicRelationshipType.withName( relParts[2] );
            }
            catch ( NumberFormatException e )
            {
                if ( relPropertyKeys != null )
                {
                    throw new IllegalStateException( "Can only set property keys once." );
                }
                if ( properties.size() > 0 )
                {
                    relPropertyKeys = parsePropertyKeys( properties );
                }
                continue;
            }
            target.createRelationship( from, to, type, getProperties( properties, relPropertyKeys ) );
            if ( ++counter % 100000 == 0 ) System.out.println( "Created " + counter + " relationships." );
        }
    }

    private List<Pair<String, String>> parsePropertyKeys( List<String> properties )
    {
        List<Pair<String, String>> result = new ArrayList<Pair<String, String>>();
        for ( String property : properties )
        {
            String[] parts = property.split( "@" );
            if ( parts.length < 2 )
            {
                result.add( Pair.of( parts[0], "String" ) );
            }
            else
            {
                result.add( Pair.of( parts[0], parts[1] ) );
            }
        }
        return result;
    }

    private Map<String, Object> getProperties( List<String> nodeParts, List<Pair<String, String>> propertyKeyLookupTable )
    {
        if ( nodeParts.isEmpty() || propertyKeyLookupTable == null )
        {
            return Collections.emptyMap();
        }
        Map<String, Object> properties = new HashMap<String, Object>();
        for ( int i = 0; i < nodeParts.size(); i++ )
        {
            Pair<String, String> pair = propertyKeyLookupTable.get( i );
            String key = pair.first();
            String value = nodeParts.get( i );
            if ( !value.isEmpty() )
            {
                properties.put( key, getPropertyValue( value, pair.other() ) );
            }
        }
        return properties;
    }

    private Object getPropertyValue( String s, String type )
    {
        if ( type.equals( "String" ) )
        {
            return s;
        }
        else if ( type.equals( "long" ) )
        {
            return Long.valueOf( s );
        }
        else if ( type.equals( "int" ) )
        {
            return Integer.valueOf( s );
        }
        else if ( type.equals( "byte" ) )
        {
            return Byte.valueOf( s );
        }
        else if ( type.equals( "short" ) )
        {
            return Short.valueOf( s );
        }
        else if ( type.equals( "char" ) )
        {
            return s.charAt( 0 );
        }
        else if ( type.equals( "boolean" ) )
        {
            return Boolean.valueOf( s );
        }
        else if ( type.equals( "double" ) )
        {
            return Double.valueOf( s );
        }
        else if ( type.equals( "float" ) )
        {
            return Float.valueOf( s );
        }
        else
        {
            throw new IllegalStateException( "Unknown type: " + type );
        }
    }
}
