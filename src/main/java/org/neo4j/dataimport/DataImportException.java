package org.neo4j.dataimport;

public class DataImportException extends RuntimeException
{
    public DataImportException( Exception e )
    {
        super(e);
    }
}
