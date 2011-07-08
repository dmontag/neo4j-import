package org.neo4j.dataimport;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
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
    private Set<String> relPropertyColumns;
    private Set<String> nodePropertyColumns;

    public JdbcImporter( Connection connection, String nodes, String rels )
    {
        this.connection = connection;
        nodesTable = nodes;
        relsTable = rels;
    }

    public static void main( String[] args ) throws SQLException, IOException
    {
        if ( args.length != 6 )
        {
            System.out.println( "Args: <target store dir> <config file>" );
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
        Map<String, ColumnProperty> columnTypes = getPropertyColumns( resultSet, getReservedNodeColumns(), nodePropertyColumns );
        while ( resultSet.next() )
        {
            target.createNode( resultSet.getLong( nodeIdColumnName ),
                buildPropertiesFromResultSet( columnTypes, resultSet ) );
        }
        statement.close();
    }

    private Set<String> getReservedNodeColumns()
    {
        Set<String> columns = new HashSet<String>();
        columns.add( nodeIdColumnName );
        return columns;
    }

    private void importRels( BatchInserter target ) throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( "SELECT * FROM " + relsTable );
        Map<String, ColumnProperty> columnTypes = getPropertyColumns( resultSet, getReservedRelColumns(), relPropertyColumns );
        while ( resultSet.next() )
        {
            target.createRelationship( resultSet.getLong( relSrcColumnName ),
                resultSet.getLong( relDestColumnName ),
                DynamicRelationshipType.withName( resultSet.getString( relTypeColumnName ) ),
                buildPropertiesFromResultSet( columnTypes, resultSet ) );
        }
        statement.close();
    }

    private Set<String> getReservedRelColumns()
    {
        Set<String> columns = new HashSet<String>();
        columns.add( relSrcColumnName );
        columns.add( relDestColumnName );
        columns.add( relTypeColumnName );
        return columns;
    }

    private Map<String, Object> buildPropertiesFromResultSet( Map<String, ColumnProperty> columnTypes, ResultSet resultSet ) throws SQLException
    {
        Map<String, Object> properties = new HashMap<String, Object>();
        for ( Map.Entry<String, ColumnProperty> columnEntry : columnTypes.entrySet() )
        {
            String key = columnEntry.getKey();
            Object value = columnEntry.getValue().getValue( resultSet );
            if ( value != null )
            {
                properties.put( key.toLowerCase(), value );
            }
        }
        return properties;
    }

    private Map<String, ColumnProperty> getPropertyColumns( ResultSet resultSet, Set<String> reservedColumns, Set<String> propertyColumns ) throws SQLException
    {
        Map<String, ColumnProperty> columnTypes = new HashMap<String, ColumnProperty>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        long columnCount = metaData.getColumnCount();
        System.out.println( String.format( "Found %d columns", columnCount ) );
        for ( int i = 1; i <= columnCount; i++ )
        {
            String columnName = metaData.getColumnName( i );
            if ( isPropertyColumn( columnName, reservedColumns, propertyColumns ) )
            {
                ColumnProperty columnType = getPropertyConverter( columnName, metaData.getColumnTypeName( i ) );
                columnTypes.put( columnName, columnType );
            }
        }
        return columnTypes;
    }

    private boolean isPropertyColumn( String columnName, Set<String> reservedColumns, Set<String> propertyColumns )
    {
        if ( propertyColumns != null )
        {
            return containsIgnoreCase( propertyColumns, columnName );
        }
        return !containsIgnoreCase( reservedColumns, columnName );
    }

    private boolean containsIgnoreCase( Collection<String> collection, String item )
    {
        for ( String element : collection )
        {
            if ( element.equalsIgnoreCase( item ) )
            {
                return true;
            }
        }
        return false;
    }

    private ColumnProperty getPropertyConverter( final String columnName, String columnType )
    {

        if ( columnType.equals( "VARCHAR" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getString( columnName );
                }
            };
        }
        else if ( columnType.equals( "BIGINT" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getLong( columnName );
                }
            };
        }
        else if ( columnType.equals( "INTEGER" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getInt( columnName );
                }
            };
        }
        else if ( columnType.equals( "TINYINT" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getByte( columnName );
                }
            };
        }
        else if ( columnType.equals( "SMALLINT" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getShort( columnName );
                }
            };
        }
        else if ( columnType.equals( "BOOLEAN" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getBoolean( columnName );
                }
            };
        }
        else if ( columnType.equals( "FLOAT" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getFloat( columnName );
                }
            };
        }
        else if ( columnType.equals( "DOUBLE" ) )
        {
            return new ColumnProperty()
            {
                @Override
                public Object getValue( ResultSet resultSet ) throws SQLException
                {
                    return resultSet.getDouble( columnName );
                }
            };
        }
        else
        {
            throw new IllegalStateException( "Unknown type: " + columnType );
        }
    }

    private interface ColumnProperty
    {
        Object getValue( ResultSet resultSet ) throws SQLException;
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

    public void setRelPropertyColumns( Set<String> relPropertyColumns )
    {
        this.relPropertyColumns = relPropertyColumns;
    }

    public void setNodePropertyColumns( Set<String> nodePropertyColumns )
    {
        this.nodePropertyColumns = nodePropertyColumns;
    }
}
