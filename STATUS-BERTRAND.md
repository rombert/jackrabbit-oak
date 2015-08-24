Bertrand's recent hacks around MultiplexingDocumentStore
--------------------------------------------------------

Added fixtures to be able to run the oak-jcr tests with that store:

    cd oak-jcr
    mvn clean test -Dnsfixtures=MEMORY_MULTI_NS -Dtest=LongPathTest
