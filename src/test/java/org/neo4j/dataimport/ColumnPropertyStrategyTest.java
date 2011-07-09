package org.neo4j.dataimport;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ColumnPropertyStrategyTest
{
    private static int dbId = 1;
    public static final String ID_COLUMN_NAME = "id";
    private Connection connection;

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
        connection = DriverManager.getConnection( "jdbc:hsqldb:mem:import" + ColumnPropertyStrategyTest.class.getName() + dbId++, "sa", "" );
    }

    @After
    public void closeJdbc() throws Exception
    {
        connection.close();
    }

    @Test
    public void shouldGenerateEmptyPropertiesForEmptyResultSet() throws SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY, name VARCHAR)" );

        ResultSet rs = query( "SELECT * FROM nodes" );

        ColumnPropertyStrategy strategy = new ColumnPropertyStrategy();
        strategy.initialize( rs, ID_COLUMN_NAME );
    }

    @Test
    public void shouldGenerateOnePropertyForResultSetWithOneColumn() throws SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY, name VARCHAR)" );
        update( "INSERT INTO nodes (id,name) VALUES(1,'hello')" );

        ResultSet rs = query( "SELECT * FROM nodes" );
        ColumnPropertyStrategy strategy = new ColumnPropertyStrategy();
        strategy.initialize( rs, ID_COLUMN_NAME );

        rs.next();
        Map<String, Object> properties = strategy.getPropertiesForCursorRow( rs );
        assertEquals( "hello", properties.get( "name" ) );
        assertFalse( properties.containsKey( "id" ) );
    }

    @Test
    public void testPropertyTypes() throws IOException, SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY, s VARCHAR, l BIGINT, i INTEGER, sh SMALLINT, b TINYINT, f FLOAT, d DOUBLE, bo BOOLEAN)" );
        update( "INSERT INTO nodes (id,s,l,i,sh,b,f,d,bo) VALUES(1,'hello',9999999999999999,888888888,777,66,0.2345,0.1234,TRUE)" );

        ResultSet rs = query( "SELECT * FROM nodes" );
        ColumnPropertyStrategy strategy = new ColumnPropertyStrategy();
        strategy.initialize( rs, ID_COLUMN_NAME );

        rs.next();
        Map<String, Object> propertiesForCursorRow = strategy.getPropertiesForCursorRow( rs );

        Assert.assertEquals( "hello", propertiesForCursorRow.get( "s" ) );
        Assert.assertEquals( 9999999999999999L, propertiesForCursorRow.get( "l" ) );
        Assert.assertEquals( 888888888, propertiesForCursorRow.get( "i" ) );
        Assert.assertEquals( (short) 777, propertiesForCursorRow.get( "sh" ) );
        Assert.assertEquals( (byte) 66, propertiesForCursorRow.get( "b" ) );
        Assert.assertEquals( 0.2345f, propertiesForCursorRow.get( "f" ) );
        Assert.assertEquals( 0.1234d, propertiesForCursorRow.get( "d" ) );
        Assert.assertEquals( true, propertiesForCursorRow.get( "bo" ) );
    }

    @Test
    public void testSparseProperties() throws IOException, SQLException
    {
        update( "CREATE TABLE nodes (id BIGINT IDENTITY, name VARCHAR, age BIGINT)" );
        update( "INSERT INTO nodes (id,name) VALUES(1,'a')" );
        update( "INSERT INTO nodes (id,age) VALUES(2,25)" );
        update( "INSERT INTO nodes (id,name,age) VALUES(3,'c',26)" );

        ResultSet rs = query( "SELECT * FROM nodes" );
        ColumnPropertyStrategy strategy = new ColumnPropertyStrategy();
        strategy.initialize( rs, ID_COLUMN_NAME );

        rs.next();
        Map<String, Object> propertiesForCursorRow = strategy.getPropertiesForCursorRow( rs );

        Assert.assertEquals( "a", propertiesForCursorRow.get( "name" ) );
        Assert.assertEquals( 0L, propertiesForCursorRow.get( "age" ) );

        rs.next();
        propertiesForCursorRow = strategy.getPropertiesForCursorRow( rs );

        assertFalse( propertiesForCursorRow.containsKey( "name" ) );
        Assert.assertEquals( 25L, propertiesForCursorRow.get( "age" ) );

        rs.next();
        propertiesForCursorRow = strategy.getPropertiesForCursorRow( rs );

        Assert.assertEquals( "c", propertiesForCursorRow.get( "name" ) );
        Assert.assertEquals( 26L, propertiesForCursorRow.get( "age" ) );
    }

    @Test
    public void shouldIgnoreSpecifiedColumns() throws SQLException
    {
        update( "CREATE TABLE rels (src BIGINT, dest BIGINT, type VARCHAR, since BIGINT)" );
        update( "INSERT INTO rels (src,dest,type,since) VALUES(1,2,'KNOWS',123)" );

        ResultSet rs = query( "SELECT * FROM rels" );
        ColumnPropertyStrategy strategy = new ColumnPropertyStrategy();
        strategy.initialize( rs, "src", "dest", "type" );

        rs.next();
        Map<String, Object> properties = strategy.getPropertiesForCursorRow( rs );
        assertFalse( properties.containsKey( "src" ) );
        assertFalse( properties.containsKey( "dest" ) );
        assertFalse( properties.containsKey( "type" ) );
        assertEquals( 123L, properties.get( "since" ) );
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
