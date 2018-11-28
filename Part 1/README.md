Description
===========

Input is  a TSV file containing one document per line. For every line, a document is created an added to the index. Field 1 is the document id, field 2 is
a document label, and field 3 is the text.

The index data is saved to disk in several files, in plaint text format. The postings data is distributed in blocks,
using the hash code of the terms to generate the key of the block where the term is being indexed. This way, the search
component doesn't need to load the whole index in memory before start searching, speeding up the search.

To Run
------------

The project is configured to compile and run tests with maven2.

We submitted all class files so you shouldn't need to rebuild. If you do, run:
```
$ mvn clean install
```

Once the project is built, to start indexing a file, use `index.sh`:
```
$ ./bin/index.sh books.tsv
```
`ìndex.sh` sets the initial JVM heap size to 1GB  and the maximum to 2GB. Allocating this amount of RAM before starting
to index speeds-up the indexing process since the application has enough free RAM to work and no time is lost later to
allocate the RAM on demand.

When the index is ready, to search a term in the index use `search.sh`:
```
$ ./bin/search.sh {terms}
```

To start the search server, open a new terminal window and execute `server.sh`:
```
$ ./bin/server.sh
```

Then in the other terminal window, instead of using `search.sh`, use `client.sh` to send queries to the server.
```
$ ./bin/client.sh {terms}
```
