hunter_config(bcos-framework VERSION 3.0.0-local
	URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/1f7c7de6e0b18486f1dc25de74bf0db148c2e429.tar.gz
    SHA1 f18a5c0c5c4e2c2e12451a0d02a90a446122fb1a
	CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/325b1c6971e3f42f527996cdf990b722a5eb4d50.tar.gz
    SHA1 06b904725472b1063c6cfc34894cd1979a22ad8a
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON URL_BASE=${URL_BASE}
)

