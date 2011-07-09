package org.neo4j.dataimport;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
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
    private PropertyStrategy nodePropertyStrategy = new ColumnPropertyStrategy();
    private PropertyStrategy relPropertyStrategy = new ColumnPropertyStrategy();

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
        nodePropertyStrategy.initialize( resultSet, nodeIdColumnName );
        while ( resultSet.next() )
        {
            target.createNode( resultSet.getLong( nodeIdColumnName ),
                nodePropertyStrategy.getPropertiesForCursorRow( resultSet ) );
        }
        statement.close();
    }

    private void importRels( BatchInserter target ) throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( "SELECT * FROM " + relsTable );
        relPropertyStrategy.initialize( resultSet, relSrcColumnName, relDestColumnName, relTypeColumnName );
        while ( resultSet.next() )
        {
            target.createRelationship( resultSet.getLong( relSrcColumnName ),
                resultSet.getLong( relDestColumnName ),
                DynamicRelationshipType.withName( resultSet.getString( relTypeColumnName ) ),
                relPropertyStrategy.getPropertiesForCursorRow( resultSet ) );
        }
        statement.close();
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

    public void setNodePropertyStrategy(PropertyStrategy nodePropertyStrategy)
    {
        this.nodePropertyStrategy = nodePropertyStrategy;
    }

    public void setRelPropertyStrategy( PropertyStrategy relPropertyStrategy )
    {
        this.relPropertyStrategy = relPropertyStrategy;
    }
}
