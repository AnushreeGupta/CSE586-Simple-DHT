# Simple DHT 

This project is a Simple DHT based on simplified version of Chord. There are main three things implemented: 

1. ID space partitioning/re-partitioning
2. Ring-based routing
3. Node joins

App has an activity and a content provider. However, the main activity is used for testing only and does not implement any DHT functionality. The content provider implements all DHT functionalities and supports insert and query operations alongwith node joins. Thus, if multiple instances of the app are run, all content provider instances form a Chord ring and serve insert/query requests in a distributed fashion according to the Chord protocol.
