package org.neo4j.dataimport;

import java.sql.ResultSet;
import java.sql.SQLException;

interface ColumnAccessor
{
    Object getValue( ResultSet resultSet ) throws SQLException;
}
