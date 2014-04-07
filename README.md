## **SketchOnStorm**
**Introduction**

Counting in streams based on _Sketches_(Probabilistic data structures) in Storm
Storm + Trident provides stateful processing of streams with support for distributed real time queries.
In many cases the states are counters over multiple dimensions of stream attributes.
Instead of counters, here in an attempt is made to back the state using sketches.

### Sketches
Sketches - Probabilistic data structures are summary data structures which store summary (a projection) of the original data set.
Sketches can store in streams summary in sub-linear Space in addition to this it supports merging(Union & intersection) operations
Merging comes in handy when dealing with discrete subsets of streams(like sliding windows)

More on sketches
http://blog.aggregateknowledge.com/

Trident topologies can be used for stateful processing of streams, the high level abstraction (_groupBy,Filters, Functions,Reducers,Aggregators_...)
provided makes is easy to manage the stateful operations(aggregation, reduce).
The state can be backed by distributed counters (like in Hbase or Cassandra).
In here, the state states are backed by a sketch
- **HyperLogLog** --> for cardinality estimation (used in unique item detection)
- **CountMinSketch** --> to keep track of frequency of known items ( and additionally to find Heavy Hitters)

In some of the use cases, the streams needs to be viewed and processed as discrete windows(example: Time bound sliding windows)
In these cases, maintaining unique counts over large windows needs to keep track of the item keys not just the counts of the windows.Storing item keys is space consuming, for example in case of maintaining a key set for tracking conversations(unique pairs of Source IP + Destination Ip) in IP Traffic Flow
could be exhaustive. Since sketches are mergeable , it possible to maintain a sketch for estimating the unique conversation without having to maintain the keys.
To support sliding windows, sketches from different windows are merged; In example cited above, to get the unique conversation count
for last one hour combine last 12 sketches or for last 3 hours combine last 12 windows and combine selectively the hourly windows from previous hours. Thus both space saving and sliding window is achieved.

### Built using

storm [0.9.1-incubating] http://storm.incubator.apache.org/

stream lib [2.7.0-SNAPSHOT] https://github.com/addthis/stream-lib

### Key classes
_**Topology definition**_

_sn.topo.UniqueUserCounterTopologyBuilder_ --> Unique user counts
_sn.topo.FlowAnalysisTopologyBuilder_ --> IP Flow analyzer to keep track of unique conversation
_sn.topo.DataVolumeAnalysisTopologyBuilder_ --> track heavy hitters(volume) given a set of hosts

**Aggregator**

_sn.customtypes.HLLAggregator_ --> HyperLogLog based aggregator
_sn.customtypes.IpConversationSketch_ --> aggregates unique IP conversations
_sn.customtypes.DataVolumeAggregator_--> aggregates the volume per host based on CountMinSketch

**Launcher & DRPC queries**

_sn.runner.UserIdCounterRunner_ --> Queries for the unique users minutely
_sn.runner.FlowAnalyzer_--> Queries for the unique conversations minutely & hourly
_sn.ruuner.DataVolumeTracker_ --> Queries for the heavy hitters minutely for a given hour
