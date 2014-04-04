## **SketchOnStorm**
###

**Introduction**

Counting in streams based on _Sketches_(Probabilistic data structures) on Storm http://storm.incubator.apache.org/
Storm + Trident provides stateful processing of streams with support for distributed real time queries of the states.In many cases the states are counters over multiple dimensions of stream attributes.
Instead of counters, here in an attempt is made to back the state using sketches.

### Sketches
Sketches - Probabilistic data structures are summary data structures which store summary (a projection) of the original data set.
Sketches can store in sublinear
Advantages of sketches include low memory foot-print, extremely fast updates & support merging
http://blog.aggregateknowledge.com/

Using Trident provided high level abstractions(groupBy,Filters, Functions...) with which stateful processing can be worked out in Storm.
For example counting of unique users, pages visited; the state is backed by distributed counters provided built with HBase or Cassanadra.
In case of counting unique users, simple counter updation doesn't work; one needs to track all the user ids seen in that batch.
When it comes to batch combine in trident, again the tracking of user ids already seen is required.
Sketches seems to be a more natural fit, unique user count problem is a cateogory cadrinality estimation.
When a user tuple is seen , the sketch is updated and on combine of batches a union of sketches is done which gives the unique user count.




