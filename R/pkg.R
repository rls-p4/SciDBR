#/*
#**
#* BEGIN_COPYRIGHT
#*
#* Copyright (C) 2008-2012 SciDB, Inc.
#*
#* SciDB is free software: you can redistribute it and/or modify it under the
#* terms of the GNU General Public License as published by the Free Software
#* Foundation version 3 of the License.
#*
#* This software is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
#* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY, NON-INFRINGEMENT, OR
#* FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for the
#* complete license terms.
#*
#* You should have received a copy of the GNU General Public License
#* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
#*
#* END_COPYRIGHT
#*/

# Package initialization and cleanup routines

.onLoad = function(libname,pkgname)
{
# Maximum allowed sequential index limit (for larger, use between)
  options(scidb.index.sequence.limit=1000000)
# Default empty fill-in value
  options(scidb.default.value=NA)
# Maximum allowed elements in an array return result
  options(scidb.max.array.elements=100000000)
}

.onUnload = function(libpath)
{
  options(scidb.index.sequence.limit=c())
  options(scidb.default.value=c())
  options(scidb.max.array.elements=c())
}

# scidb array object type map. We don't yet support strings in scidb array
# objects. Use df2scidb and iquery for strings.
# R type = SciDB type
.scidbtypes = list(
  double="double",
  integer="int32",
  logical="bool",
  character="char"
)
