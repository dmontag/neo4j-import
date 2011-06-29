package org.neo4j.dataimport;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JdbcImporter implements BatchInserterImporter
{
    private Connection connection;
    private String nodesTable;
    private String relsTable;
    private String nodeIdColumnName = "id";
    private String relSrcColumnName = "src";
    private String relDestColumnName = "dest";
    private String relTypeColumnName = "type";
    private Set<String> additionalReservedRelPropertyColumns;
    private Set<String> additionalReservedNodePropertyColumns;

    public JdbcImporter( Connection connection, String nodes, String rels )
    {
        this.connection = connection;
        nodesTable = nodes;
        relsTable = rels;
    }

    public void setNodeIdColumnName( String nodeIdColumnName )
    {
        this.nodeIdColumnName = nodeIdColumnName;
    }

    public void setRelSrcColumnName( String relSrcColumnName )
    {
        this.relSrcColumnName = relSrcColumnName;
    }

    public void setRelDestColumnName( String relDestColumnName )
    {
        this.relDestColumnName = relDestColumnName;
    }

    public void setRelTypeColumnName( String relTypeColumnName )
    {
        this.relTypeColumnName = relTypeColumnName;
    }

    public void setAdditionalReservedRelPropertyColumns( Set<String> additionalReservedRelPropertyColumns )
    {
        this.additionalReservedRelPropertyColumns = additionalReservedRelPropertyColumns;
    }

    public void setAdditionalReservedNodePropertyColumns( Set<String> additionalReservedNodePropertyColumns )
    {
        this.additionalReservedNodePropertyColumns = additionalReservedNodePropertyColumns;
    }

    public static void main(String[] args) throws SQLException
    {
        if ( args.length != 6 )
        {
            System.out.println( "Args: <target store dir> <connection string> <user> <pass> <nodes table> <relationships table>" );
            System.exit( 1 );
        }
        String storeDir = args[0];
        String connectionString = args[1];
        String user = args[2];
        String pass = args[3];
        String nodesTable = args[4];
        String relsTable = args[5];
        BatchInserter batchInserter = new BatchInserterImpl( storeDir, getConfig( storeDir ) );
        try
        {
            new JdbcImporter( DriverManager.getConnection( connectionString, user, pass ), nodesTable, relsTable ).importTo( batchInserter );
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
        Map<String, String> columnTypes = getPropertyColumns( resultSet, getReservedNodeColumns() );
        while ( resultSet.next() )
        {
            target.createNode( resultSet.getLong( nodeIdColumnName ), getProperties( columnTypes, resultSet ) );
        }
        statement.close();
    }

    private Set<String> getReservedNodeColumns()
    {
        Set<String> columns = additionalReservedNodePropertyColumns != null ? new HashSet<String>( additionalReservedNodePropertyColumns ) : new HashSet<String>();
        columns.add( nodeIdColumnName );
        return columns;
    }

    private void importRels( BatchInserter target ) throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( "SELECT * FROM " + relsTable );
        Map<String, String> columnTypes = getPropertyColumns( resultSet, getReservedRelColumns() );
        while ( resultSet.next() )
        {
            target.createRelationship( resultSet.getLong( relSrcColumnName ), resultSet.getLong( relDestColumnName ), DynamicRelationshipType.withName( resultSet.getString( relTypeColumnName ) ), getProperties( columnTypes, resultSet ) );
        }
        statement.close();
    }

    private Set<String> getReservedRelColumns()
    {
        Set<String> columns = additionalReservedNodePropertyColumns != null ? new HashSet<String>( additionalReservedRelPropertyColumns ) : new HashSet<String>();
        columns.add( relSrcColumnName );
        columns.add( relDestColumnName );
        columns.add( relTypeColumnName );
        return columns;
    }

    private Map<String, String> getPropertyColumns( ResultSet resultSet, Set<String> reserved ) throws SQLException
    {
        Map<String, String> columnTypes = new HashMap<String, String>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        long columnCount = metaData.getColumnCount();
        System.out.println( String.format( "Found %d columns", columnCount ) );
        for ( int i = 1; i <= columnCount; i++ )
        {
            String columnName = metaData.getColumnName( i ).toLowerCase();
            String columnType = metaData.getColumnTypeName( i ).toLowerCase();
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
                properties.put( key, value );
            }
        }
        return properties;
    }

    private Object getProperty( String column, String type, ResultSet resultSet ) throws SQLException
    {
        if ( type.equals( "varchar" ) )
        {
            return resultSet.getString( column );
        }
        else if ( type.equals( "bigint" ) )
        {
            return resultSet.getLong( column );
        }
        else if ( type.equals( "integer" ) )
        {
            return resultSet.getInt( column );
        }
        else if ( type.equals( "tinyint" ) )
        {
            return resultSet.getByte( column );
        }
        else if ( type.equals( "smallint" ) )
        {
            return resultSet.getShort( column );
        }
//        else if ( type.equals( "char" ) )
//        {
//            return s.charAt( 0 );
//        }
        else if ( type.equals( "boolean" ) )
        {
            return resultSet.getBoolean( column );
        }
        else if ( type.equals( "float" ) )
        {
            return resultSet.getFloat( column );
        }
        else if ( type.equals( "double" ) )
        {
            return resultSet.getDouble( column );
        }
        else
        {
            throw new IllegalStateException( "Unknown type: " + type );
        }
    }
}
