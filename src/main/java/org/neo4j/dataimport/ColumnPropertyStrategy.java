package org.neo4j.dataimport;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ColumnPropertyStrategy implements PropertyStrategy
{

    private String nodeIdColumnName = "id";
    private Set<String> nodePropertyColumns;
    private Map<String,ColumnAccessor> columnTypes;

    @Override
    public void initialize( ResultSet resultSet, String... reservedColumns ) throws SQLException
    {
        nodeIdColumnName = reservedColumns[0];
        columnTypes = getPropertyColumns( resultSet, getReservedNodeColumns(), nodePropertyColumns );
    }

    @Override
    public Map<String, Object> getPropertiesForCursorRow( ResultSet resultSet ) throws SQLException
    {

        Map<String, Object> properties = new HashMap<String, Object>();
        for ( Map.Entry<String, ColumnAccessor> columnEntry : columnTypes.entrySet() )
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

    private Set<String> getReservedNodeColumns()
    {
        Set<String> columns = new HashSet<String>();
        columns.add( nodeIdColumnName );
        return columns;
    }

    private Map<String, ColumnAccessor> getPropertyColumns( ResultSet resultSet, Set<String> reservedColumns, Set<String> propertyColumns ) throws SQLException
    {
        Map<String, ColumnAccessor> columnTypes = new HashMap<String, ColumnAccessor>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        long columnCount = metaData.getColumnCount();
        System.out.println( String.format( "Found %d columns", columnCount ) );
        for ( int i = 1; i <= columnCount; i++ )
        {
            String columnName = metaData.getColumnName( i );
            if ( isPropertyColumn( columnName, reservedColumns, propertyColumns ) )
            {
                ColumnAccessor columnType = getPropertyConverter( columnName, metaData.getColumnTypeName( i ) );
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

    private ColumnAccessor getPropertyConverter( final String columnName, String columnType )
    {

        if ( columnType.equals( "VARCHAR" ) )
        {
            return new ColumnAccessor()
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
            return new ColumnAccessor()
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
            return new ColumnAccessor()
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
            return new ColumnAccessor()
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
            return new ColumnAccessor()
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
            return new ColumnAccessor()
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
            return new ColumnAccessor()
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
            return new ColumnAccessor()
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

    public void setNodePropertyColumns( Set<String> nodePropertyColumns )
    {
        this.nodePropertyColumns = nodePropertyColumns;
    }
}

