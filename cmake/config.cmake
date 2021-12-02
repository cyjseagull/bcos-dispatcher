hunter_config(bcos-framework VERSION 3.0.0-local
	URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/1f366fe57a47ec7ee27569b07d3a0cc6be2ea7c2.tar.gz
    SHA1 f2908bf61ab51cdd4ed5752781a35d391e1221c2
	CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/6bc78eee8426acca811d154ca87b213cf8ba859a.tar.gz
    SHA1 38ca3f1606583cd25ec1a2ce6cb714a772f0f3ec
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON URL_BASE=${URL_BASE}
)
