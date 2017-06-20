Interation test for issue https://github.com/orientechnologies/orientdb/issues/7186 

Test is split on several phases:

Phase 1

Create 10 400 000 vertexes in batches. In each batch create 104 000 vertexes.
1 000 vetexes for each vertex type. At the end of each batch close and reopen storage.

Vertexes for all vetex types are added concurrently in 8 threads.


Phase 2

Find one vertex and find related 100 vertexes create edge of single type between them.
Peform this step for all 104 edge types.

All edges for all edge types are created concurrently in 8 threads.

After insertion of 1 000 edges for each edge type (104 000 edges in total) close the storage and reopen it again.
Repeat this step 1 000 times. So 104 000 000 of edges are created in total on this phase.

Phase 3.

Remove 5 200 000 vertexes.
Split this process by batches, in each butch remove 104 000 vertexes. So 50 batches are performed.
Vertexes of all vertex types are removed randomly and concurrently in 8 threads.

Phase 4.

Add 5 200 000 vertexes.
For that add 50 000 vertexes for each vertex type.
Split process by batches in each butch add 1 000 vertexes for each vertex type. At the end of each batch open and close storage.

Vertexes of all vertex types are added concurrently in 8 threads.


Phase 5.

Add 52 000 000 edges.
For that add 500 000 edges for each edge type.
Split process by batches in each butch add 1 000 edges for each edge type. At the end of each batch open and close storage.
As result 500 batches are performed.

Repeat phase 3 - 5 ten times.