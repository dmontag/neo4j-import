package org.neo4j.dataimport;

import org.neo4j.kernel.impl.batchinsert.BatchInserter;

public interface BatchInserterImporter
{
    void importTo( BatchInserter target );
}
