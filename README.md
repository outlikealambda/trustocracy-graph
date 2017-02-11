## Maintaining a Deterministic Connection State in a Directed, Cyclic Graph with Weighted Edges

### Overview

These procedures expose specific Neo4j graph actions (see either @CleanConnectivity.java or @DirtyConnectivity.java for the procedures) for consumption via Cypher calls.  The exposed actions trigger updates to the state of connectivity when edges of the graph are modified.

**Basic Graph Structure**

Nodes:
- Person
- Opinion

Edges:
- Ranked (Person -> Person)
- Authored (Person -> Opinion)
- Connected (Person -> (Person || Opinion))

A "connection" is defined as a path which leads from a Person, through any amount of other Persons, to an Opinion.  If there is no path, then that Person is not connected.  The possible connection states for a Person are:

Connected:
- Manually select a connected Person
- Automatically connect to the adjoining Person with (the lowest weighted edge && is connected)

Disjoint:
- Manually select a disjoint Person
- All adjoining Persons are disjoint
- Either connected state above creates a cycle

### Setup

To run neo4j with the plugin:

- build the project
- (optional) copy the jar file to a separate folder, location of your choosing
- find the neo4j.conf file (if using homebrew and neo4j this is at `/usr/local/Cellar/neo4j/<version>/libexec/conf/neo4j.conf`)
- add/modify the line to point to the location of the compiled jar: `dbms.directories.plugins=/path/to/pluginsdirectory`
- start neo4j

### Currently Available Cypher Procedures

- `CALL dirty.target.set({sourceId}, {targetId}, {topicId})` - manually selects a Person, removing any previous selection
- `CALL dirty.target.clear({sourceId}, {topicId})` - clears any manual selection
- `CALL dirty.opinion.set({authorId}, {opinionId}, {topicId})` - connects a Person and Opinion, removing any previous opinion
- `CALL dirty.opinion.clear({authorId}, {topicId})` - clears any Authored connection
- `CALL dirty.ranked.set({sourceId}, [{targetId}...])` - sets the adjoining neighbors, ranked by index (0 is closest)
- `CALL friend.author.opinion({sourceId})` - returns a list of adjoining Persons, and the Author + Opinion they are connected to (however far away it may be).  The path to the opinion is deliberately omitted.
- `CALL measure.influence({sourceId}, {topicId}` - recursively count the number of nodes connected to the source for a given topic

- Note: replacing `dirty` with `clean` will give you the same endpoints, but with a slower, more provably correct algorithm which we use(d) to verify the dirty algorithm.
