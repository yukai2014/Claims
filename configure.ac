AC_INIT([Claims], [0.3], [imdb@ecnu])
AM_INIT_AUTOMAKE([-Wall foreign subdir-objects])
AM_PROG_AR
AC_PROG_LIBTOOL
AM_CONDITIONAL(OPT_TCMALLOC, true)
AM_CONDITIONAL(OPT_DEBUG, false)

CPPFLAGS="-w -O2 -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS"
if OPT_DEBUG 
then
CPPFLAGS+="-g -DTHERON_XS"
else
CPPFLAGS+=" -DNDEBUG"
fi

CXXFLAGS=${CXXFLAGS=" -std=c++11"}
AC_PROG_CXX
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([
  Makefile
  catalog/Makefile
  catalog/Test/Makefile
  catalog/stat/Makefile 
  Client/Makefile 
  common/Makefile 
  common/Block/Makefile 
  common/Expression/Makefile 
  common/expression/Makefile
  common/file_handle/Makefile
  common/file_handle/test/Makefile
  common/log/Makefile
  common/Schema/Makefile 
  common/Schema/Test/Makefile 
  common/types/Makefile 
  common/types/Test/Makefile 
  common/types/ttmath/Makefile 
  Daemon/Makefile 
  Executor/Makefile 
  Executor/Test/Makefile 
  IndexManager/Makefile
  IndexManager/Test/Makefile
  loader/Makefile
  loader/test/Makefile
  common/serialization/Makefile 
  physical_operator/Makefile
  logical_operator/Makefile 
  Resource/Makefile 
  storage/Makefile 
  sql_parser/Makefile
  sql_parser/ast_node/Makefile
  sql_parser/parser/Makefile
  sql_parser/Test/Makefile
  Test/Makefile
  Test/common/Makefile
  Test/utility/Makefile
  txn_manager/Makefile
  utility/Makefile
  codegen/Makefile 
  stmt_handler/Makefile
])
AC_OUTPUT

