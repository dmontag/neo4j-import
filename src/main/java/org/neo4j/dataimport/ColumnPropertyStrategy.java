package org.neo4j.dataimport;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ColumnPropertyStrategy implements PropertyStrategy
{

    private Set<String> reservedColumns = new HashSet<String>();
    private Set<String> specificPropertyColumns;
    private Map<String, ColumnAccessor> columnTypes = new HashMap<String, ColumnAccessor>();

    public ColumnPropertyStrategy()
    {
    }

    public ColumnPropertyStrategy( String... specificPropertyColumns )
    {
        this.specificPropertyColumns = asSet( specificPropertyColumns );
    }

    @Override
    public void initialize( ResultSet resultSet, String... reservedColumns ) throws SQLException
    {
        this.reservedColumns = asSet( reservedColumns );
        columnTypes = getPropertyColumns( resultSet );
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

    private Map<String, ColumnAccessor> getPropertyColumns( ResultSet resultSet ) throws SQLException
    {
        Map<String, ColumnAccessor> columnTypes = new HashMap<String, ColumnAccessor>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        long columnCount = metaData.getColumnCount();
        System.out.println( String.format( "Found %d columns", columnCount ) );
        for ( int i = 1; i <= columnCount; i++ )
        {
            String columnName = metaData.getColumnName( i );
            if ( isPropertyColumn( columnName ) )
            {
                ColumnAccessor columnType = getPropertyConverter( columnName, metaData.getColumnTypeName( i ) );
                columnTypes.put( columnName, columnType );
            }
        }
        return columnTypes;
    }

    private boolean isPropertyColumn( String columnName )
    {
        if ( specificPropertyColumns != null )
        {
            return containsIgnoreCase( specificPropertyColumns, columnName );
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
        else if ( columnType.equals( "INTEGER" ) || columnType.equals( "INT" ))  //change for mysql int range colum
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

//    public void setSpecificPropertyColumns( String... specificPropertyColumns )
//    {
//        this.specificPropertyColumns = asSet( specificPropertyColumns );
//    }

    private static Set<String> asSet( String... reservedColumns )
    {
        return new HashSet<String>( Arrays.asList( reservedColumns ) );
    }
}

