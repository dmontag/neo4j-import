package org.neo4j.dataimport;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JdbcImporter implements BatchInserterImporter
{
    private Connection connection;
    private String nodesTable;
    private String relsTable;
    private Set<String> reservedRelColumns = new HashSet<String>( Arrays.asList( "src", "dest", "type" ) );
    private Set<String> reservedNodeColumns = new HashSet<String>( Arrays.asList( "id" ) );


    public JdbcImporter( Connection connection, String nodes, String rels )
    {
        this.connection = connection;
        nodesTable = nodes;
        relsTable = rels;
    }

    @Override
    public void importTo( BatchInserter target )
    {
        try
        {
            doImport( target );
        }
        catch ( SQLException e )
        {
            throw new DataImportException( e );
        }
    }

    private void doImport( BatchInserter target ) throws SQLException
    {
        importNodes( target );
        importRels( target );
    }

    private void importNodes( BatchInserter target ) throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( "SELECT * FROM " + nodesTable );
        Map<String, String> columnTypes = getPropertyColumns( resultSet, reservedNodeColumns );
        while ( resultSet.next() )
        {
            target.createNode( resultSet.getLong( "id" ), getProperties( columnTypes, resultSet ) );
        }
        statement.close();
    }

    private void importRels( BatchInserter target ) throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( "SELECT * FROM " + relsTable );
        Map<String, String> columnTypes = getPropertyColumns( resultSet, reservedRelColumns );
        while ( resultSet.next() )
        {
            target.createRelationship( resultSet.getLong( "src" ), resultSet.getLong( "dest" ), DynamicRelationshipType.withName( resultSet.getString( "type" ) ), getProperties( columnTypes, resultSet ) );
        }
        statement.close();
    }

    private Map<String, String> getPropertyColumns( ResultSet resultSet, Set<String> reserved ) throws SQLException
    {
        Map<String, String> columnTypes = new HashMap<String, String>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        long columnCount = metaData.getColumnCount();
        System.out.println( String.format( "Found %d columns", columnCount ) );
        for ( int i = 1; i <= columnCount; i++ )
        {
            String columnName = metaData.getColumnName( i );
            String columnType = metaData.getColumnTypeName( i );
            System.out.println( String.format( "Found column %s (%s)", columnName, columnType ) );
            if ( !reserved.contains( columnName ) )
            {
                columnTypes.put( columnName, columnType );
            }
        }
        return columnTypes;
    }

    private Map<String, Object> getProperties( Map<String, String> columnTypes, ResultSet resultSet ) throws SQLException
    {
        Map<String, Object> properties = new HashMap<String, Object>();
        for ( Map.Entry<String, String> columnEntry : columnTypes.entrySet() )
        {
            String key = columnEntry.getKey();
            Object value = getProperty( key, columnEntry.getValue(), resultSet );
            if ( value != null )
            {
                properties.put( key.toLowerCase(), value );
            }
        }
        System.out.println( "props: " + properties );
        return properties;
    }

    private Object getProperty( String column, String type, ResultSet resultSet ) throws SQLException
    {
        if ( type.equals( "VARCHAR" ) )
        {
            return resultSet.getString( column );
        }
        else if ( type.equals( "BIGINT" ) )
        {
            return resultSet.getLong( column );
        }
        else if ( type.equals( "INTEGER" ) )
        {
            return resultSet.getInt( column );
        }
        else if ( type.equals( "TINYINT" ) )
        {
            return resultSet.getByte( column );
        }
        else if ( type.equals( "SMALLINT" ) )
        {
            return resultSet.getShort( column );
        }
//        else if ( type.equals( "char" ) )
//        {
//            return s.charAt( 0 );
//        }
        else if ( type.equals( "BOOLEAN" ) )
        {
            return resultSet.getBoolean( column );
        }
        else if ( type.equals( "FLOAT" ) )
        {
            return resultSet.getFloat( column );
        }
        else if ( type.equals( "DOUBLE" ) )
        {
            return resultSet.getDouble( column );
        }
        else
        {
            throw new IllegalStateException( "Unknown type: " + type );
        }
    }

}
