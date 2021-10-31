hunter_config(bcos-framework VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/ea79991a192fd4c0db0a1a98df918c08765dc549.tar.gz
    SHA1 4f65fb905f18e14df5da03c4e11bde22b4e93ecb
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON #DEBUG=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/20cfeb18559aa051d59966250670619e2a8ff0ba.tar.gz
    SHA1 9cc34ea674da9d21d52536f2d4238896e5958ceb
    CMAKE_ARGS URL_BASE=${URL_BASE}
)
