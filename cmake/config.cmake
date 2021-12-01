hunter_config(bcos-framework VERSION 3.0.0-local
	URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/2a3beb81f02fc4c5cdd39c333ae5021b12a6d708.tar.gz
	SHA1 9bd7b4be1511ce6321f8d4b26c1dff8af11e3729
	CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/6bc78eee8426acca811d154ca87b213cf8ba859a.tar.gz
    SHA1 38ca3f1606583cd25ec1a2ce6cb714a772f0f3ec
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON URL_BASE=${URL_BASE}
)
