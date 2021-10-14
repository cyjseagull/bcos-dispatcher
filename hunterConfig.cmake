hunter_config(bcos-framework VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/3c4ec4971c88b0caa96f8b5e7acdd05fba9e6c3a.tar.gz
    SHA1 b20b2e9a21fddbb8a3ac28770e5274873a31cbe9
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON #DEBUG=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/d6ef97d286ec88de3f9ac874730f9b98916bc2fc.tar.gz
    SHA1 29b4c5fdca7fd3d0b3ec95a1015f12b099eb2af2
    CMAKE_ARGS URL_BASE=${URL_BASE}
)