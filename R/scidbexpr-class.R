#/*
#**
#* BEGIN_COPYRIGHT
#*
#* This file is part of SciDB.
#* Copyright (C) 2008-2013 SciDB, Inc.
#*
#* SciDB is free software: you can redistribute it and/or modify
#* it under the terms of the AFFERO GNU General Public License as published by
#* the Free Software Foundation.
#*
#* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
#* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
#* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
#* the AFFERO GNU General Public License for the complete license terms.
#*
#* You should have received a copy of the AFFERO GNU General Public License
#* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#*
#* END_COPYRIGHT
#*/

# This file contains class definitions, generics, and methods for the
# aflexpr class, which is really just a character string that represents
# an afl expression. We use this class instead of character to differentiate
# it for functions like 'merge.'

scidbexpr = setClass("scidbexpr", representation=list(lastclass="character"), contains = "character")
setMethod("aggregate", signature(x="scidbexpr"), aggregate_scidb)

# Construct a virtual scidb object reference from a SciDB expression.
# The underlying SciDB object in this case does not exist.
scidb_from_scidbexpr = function(x)
{
  s = iquery(sprintf("show('%s','afl')",x),`return`=TRUE)[[2]]
  scidb_from_schemastring(s,x)
}
