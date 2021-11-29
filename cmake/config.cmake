hunter_config(bcos-framework VERSION 3.0.0-local
	URL https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/1dc0032b86a45fe5c87add053335734583efdcf7.tar.gz
    SHA1 b0686afb71d532188f0c8fc879236f0ec3403739
	CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(bcos-tars-protocol
    VERSION 3.0.0-local
    URL https://${URL_BASE}/FISCO-BCOS/bcos-tars-protocol/archive/502c17b20558f4fd3809135622ca68c71b8ff16d.tar.gz
    SHA1 7f94aa82374b3dbbeaa10af056fcea14bd72720a
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON URL_BASE=${URL_BASE}
)
