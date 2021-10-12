hunter_config(bcos-framework VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/11c5e1d07bc80cbc3b58b178dd220cc0dc601dc9.tar.gz
    SHA1 1c5a741592445004e0ded6714b973b1ba897475d
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON #DEBUG=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/d6ef97d286ec88de3f9ac874730f9b98916bc2fc.tar.gz
    SHA1 29b4c5fdca7fd3d0b3ec95a1015f12b099eb2af2
    CMAKE_ARGS URL_BASE=${URL_BASE}
)