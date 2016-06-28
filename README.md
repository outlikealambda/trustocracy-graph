## Overview

This is a distance limited traversal plugin for Neo4j, built off of the example code from https://github.com/neo4j-examples/neo4j-procedure-template

## Setup

To run neo4j with the plugin:

- build the project
- (optional) copy the jar file to a separate folder, location of your choosing
- find the neo4j.conf file (if using homebrew, and neo4j v3.0.3, this is at `/usr/local/Cellar/neo4j/3.0.3/libexec/conf/neo4j.conf`)
- add/modify the line to point to the location of the compiled jar: `dbms.directories.plugins=/path/to/file.jar` 
- start neo4j
- the distance traversal should be available via the cypher query `CALL traverse.distance({userId}, {topicId}, {maxDistance})`
