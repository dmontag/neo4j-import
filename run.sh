#!/bin/sh

java -server -Xmx2048m -cp target/neo4j-import-1.0.jar:target/dependency/\* org.neo4j.dataimport.CsvImporter $*
