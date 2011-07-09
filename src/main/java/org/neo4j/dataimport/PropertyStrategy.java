package org.neo4j.dataimport;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface PropertyStrategy
{
    void initialize( ResultSet resultSet, String... reservedColumns ) throws SQLException;

    Map<String,Object> getPropertiesForCursorRow( ResultSet resultSet ) throws SQLException;
}
