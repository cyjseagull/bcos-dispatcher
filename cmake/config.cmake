hunter_config(bcos-framework VERSION 3.0.0-local
	URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/bba5fd46533a3ec33cf608ad0bbef0d2bb822cc1.tar.gz
    SHA1 0d11165cb034cb1b6b7678c92266acea5c21f59b
	CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/41bdfe5675ccf831883deb9017a3bf11eff1792a.tar.gz
    SHA1 a1ecc48f032bd31ee3373eb85e0e2e092797bad8
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON URL_BASE=${URL_BASE}
)
