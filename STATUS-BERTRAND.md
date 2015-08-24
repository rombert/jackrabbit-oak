Bertrand's recent hacks around MultiplexingDocumentStore
--------------------------------------------------------

Added fixtures to be able to run the oak-jcr tests with that store:

    cd oak-jcr
    mvn clean test -Dnsfixtures=MEMORY_MULTI_NS -Dtest=LongPathTest
    
That `LongPathTest` currently fails as `MultiplexingDocumentStore.asDocumentKey(...)` is called with a key string that's a hashed version of a long path. That hash does not provide the required info to select the appropriate store.

To fix this we might introduce a `DocumentKey` object that always keeps the path that corresponds to the possibly hashed document key. This was discussed at http://markmail.org/message/ztpm5rwggmsvwy3r , the Oak folks did not seem too enthusiastic as that entails a lot of small changes. But that's probably the cleanest way.

Apart from that all `oak-jcr` tests pass with that fixture, after fixing (IMO) `MultiplexingDocumentStore.create` to return true only if all its stores return true.
