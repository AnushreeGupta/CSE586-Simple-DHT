# Simple DHT 

This project is a Simple DHT based on simplified version of Chord. There are main three things implemented: 

1. ID space partitioning/re-partitioning
2. Ring-based routing
3. Node joins

App has an activity and a content provider. However, the main activity is used for testing only and does not implement any DHT functionality. The content provider implements all DHT functionalities and supports insert and query operations. Thus, if multiple instances of the app are run, all content provider instances form a Chord ring and serve insert/query requests in a distributed fashion according to the Chord protocol.

Apart from above funtions the app is be able to handle node joins. Only the first emulator instance (i.e., emulator-5554) receives all new node join requests. Upon completing a new node join request, affected nodes are updated with their predecessor and successor pointers correctly. Below are the assumptions taken into consideration while writing the content provider. 

a. A node join will only happen once the system completely processes the previous join i.e., no concurrent node joins.
b. Insert/query requests will be issued only with a stable system thus while a node is joining no insert/query requests are recieved.
c. The content provider does not need to handle node leaves/failures. 
