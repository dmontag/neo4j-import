package org.neo4j.dataimport;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    @Override
    public void importTo( BatchInserter target )
    {
        final LuceneBatchInserterIndexProvider batchInserter = new LuceneBatchInserterIndexProvider( target );
        try
        {
            importNodes( target, batchInserter );
            importRels( target );
        }
        catch ( Exception e )
        {
            throw new DataImportException( e );
        }
        finally {
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

    private void importNodes( BatchInserter target, LuceneBatchInserterIndexProvider indexProvider ) throws FileNotFoundException
    {
        List<PropertyKey> nodePropertyKeys = null;
        Collection<IndexEntry> indices = null;
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
                    indices = configureIndices( nodePropertyKeys, indexProvider );
                }
                continue;
            }
            final Map<String, Object> props = getProperties( properties, nodePropertyKeys );
            target.createNode( id, props );
            indexProperties( id, indices, props );
            if ( ++counter % 100000 == 0 ) System.out.println( "Created " + counter + " nodes." );
        }
    }

    private void indexProperties( long id, Collection<IndexEntry> indices, Map<String, Object> props )
    {
        if ( indices == null ) return;
        for ( IndexEntry indexEntry : indices )
        {
            Map<String, Object> indexedKeys = new HashMap<String, Object>();
            for ( String key : indexEntry.getKeys() )
            {
                indexedKeys.put( key, props.get( key ) );
            }
            indexEntry.getIndex().add( id, indexedKeys );
        }
    }

    private Collection<IndexEntry> configureIndices( List<PropertyKey> nodePropertyKeys, LuceneBatchInserterIndexProvider indexProvider )
    {
        Map<String, IndexEntry> indices = new HashMap<String, IndexEntry>();
        for ( PropertyKey propertyKey : nodePropertyKeys )
        {
            if ( propertyKey.isIndexed() )
            {
                IndexEntry indexEntry = indices.get( propertyKey.getIndex() );
                if (indexEntry == null)
                {
                    indexEntry = new IndexEntry( indexProvider.nodeIndex( propertyKey.getIndex(), MapUtil.stringMap( "type", "exact" ) ) );
                    indices.put( propertyKey.getIndex(), indexEntry );
                }
                indexEntry.addKey( propertyKey.getName() );
            }
        }
        return indices.values();
    }

    private void importRels( BatchInserter target ) throws FileNotFoundException
    {
        List<PropertyKey> relPropertyKeys = null;
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

    private List<PropertyKey> parsePropertyKeys( List<String> properties )
    {
        List<PropertyKey> result = new ArrayList<PropertyKey>();
        for ( String property : properties )
        {
            result.add( new PropertyKey( parseName(property), parseType(property), parseIndex(property) ) );
        }
        return result;
    }

    private String parseIndex( String property )
    {
        final String[] parts = property.split( "\\|" );
        return parts.length > 1 ? parts[0] : null;
    }

    private PropertyType parseType( String property )
    {
        final String[] parts = property.split( "@" );
        return parts.length > 1 ? PropertyType.parseType( parts[1] ) : PropertyType.stringType;
    }

    private String parseName( String property )
    {
        final String[] parts = property.split( "@" )[0].split( "\\|" );
        return parts.length > 1 ? parts[1] : parts[0];
    }

    private Map<String, Object> getProperties( List<String> nodeParts, List<PropertyKey> propertyKeyLookupTable )
    {
        if ( nodeParts.isEmpty() || propertyKeyLookupTable == null )
        {
            return Collections.emptyMap();
        }
        Map<String, Object> properties = new HashMap<String, Object>();
        for ( int i = 0; i < nodeParts.size(); i++ )
        {
            PropertyKey propertyKey = propertyKeyLookupTable.get( i );
            String key = propertyKey.getName();
            String value = nodeParts.get( i );
            if ( !value.isEmpty() )
            {
                final Object propertyValue = getPropertyValue( value, propertyKey.getType() );
                properties.put( key, propertyValue );
            }
        }
        return properties;
    }

    private Object getPropertyValue( String s, PropertyType type )
    {
        if ( type == PropertyType.stringType )
        {
            return s;
        }
        else if ( type == PropertyType.longType )
        {
            return Long.valueOf( s );
        }
        else if ( type == PropertyType.intType )
        {
            return Integer.valueOf( s );
        }
        else if ( type == PropertyType.byteType )
        {
            return Byte.valueOf( s );
        }
        else if ( type == PropertyType.shortType )
        {
            return Short.valueOf( s );
        }
        else if ( type == PropertyType.charType )
        {
            return s.charAt( 0 );
        }
        else if ( type == PropertyType.booleanType )
        {
            return Boolean.valueOf( s );
        }
        else if ( type == PropertyType.doubleType )
        {
            return Double.valueOf( s );
        }
        else if ( type == PropertyType.floatType )
        {
            return Float.valueOf( s );
        }
        else
        {
            throw new IllegalStateException( "Unknown type: " + type );
        }
    }

    enum PropertyType
    {
        stringType, longType, intType, byteType, shortType, charType, booleanType, doubleType, floatType;

        static PropertyType parseType( String name )
        {
            return valueOf( name.toLowerCase() + "Type" );
        }
    }

    private class PropertyKey
    {
        private String name;
        private PropertyType type;
        private String index;

        private PropertyKey( String name, PropertyType type )
        {
            this( name, type, null );
        }

        private PropertyKey( String name, PropertyType type, String index )
        {
            this.name = name;
            this.type = type;
            this.index = index;
        }

        public String getName()
        {
            return name;
        }

        public PropertyType getType()
        {
            return type;
        }
        
        public String getIndex() 
        {
            return index;
        }

        public boolean isIndexed()
        {
            return index != null;
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append( "PropertyKey" );
            sb.append( "{name='" ).append( name ).append( '\'' );
            sb.append( ", type=" ).append( type );
            sb.append( ", index='" ).append( index ).append( '\'' );
            sb.append( '}' );
            return sb.toString();
        }
    }

    private class IndexEntry
    {
        private List<String> keys = new ArrayList<String>(  );
        private BatchInserterIndex index;

        public IndexEntry( BatchInserterIndex index )
        {
            this.index = index;
        }

        public void addKey( String name )
        {
            keys.add( name );
        }

        public List<String> getKeys()
        {
            return keys;
        }

        public BatchInserterIndex getIndex()
        {
            return index;
        }
    }
}
