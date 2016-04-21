AM_CPPFLAGS= -fPIC -fpermissive 

AM_LDFLAGS=-lc -lm -lrt  -ll -ly -lstdc++ -lxs -lcaf_core -lcaf_io

if OPT_TCMALLOC
AM_CPPFLAGS+=-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free 
AM_LDFLAGS+=-ltcmalloc 
endif

LDADD = ../catalog/libcatalog.a \
		../catalog/stat/libstat.a \
		../loader/libloader.a \
		../common/libcommon.a \
		../common/expression/libexpression.a \
		../utility/libutility.a \
		../sql_parser/ast_node/libast_node.a \
		../sql_parser/parser/libparser.a \
		${BOOST_HOME}/stage/lib/libboost_system.a \
		${BOOST_HOME}/stage/lib/libboost_system.so \
		${BOOST_HOME}/stage/lib/libboost_serialization.a \
		${BOOST_HOME}/stage/lib/libboost_serialization.a

noinst_LIBRARIES=libtxnmanager.a

libtxnmanager_a_SOURCES = \
	  txn.hpp txn.cpp \
	  txn_client.hpp txn_client.cpp \
	  txn_server.hpp txn_server.cpp  \
	  txn_log.hpp txn_log.cpp  \
	  log_client.hpp log_client.cpp \
	  log_server.hpp log_server.cpp 

	

	
