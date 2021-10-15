hunter_config(bcos-framework VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/b16ba5bbc083b1ae1cfb78a3fe539757610bf528.tar.gz
    SHA1 3018eceb4e12098af08db7dd5985bd911b673182
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON #DEBUG=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/d6ef97d286ec88de3f9ac874730f9b98916bc2fc.tar.gz
    SHA1 29b4c5fdca7fd3d0b3ec95a1015f12b099eb2af2
    CMAKE_ARGS URL_BASE=${URL_BASE}
)