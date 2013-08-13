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

scidbexpr = setClass("scidbexpr", contains = "character")
setMethod("aggregate", signature(x="scidbexpr"), aggregate.scidb)

# Construct a virtual scidb object reference from a SciDB expression.
# The underlying SciDB object in this case does not exist.
scidb_from_scidbexpr = function(x)
{
  s = iquery(sprintf("show('%s','afl')",x),re=TRUE)[[2]]
  scidb_from_schemastring(s,x)
}

# Construct a virtual scidb object reference from a SciDB schema string.
# The underlying SciDB object in this case does not exist.
scidb_from_schemastring = function(s,expr=character())
{
  a=strsplit(strsplit(strsplit(strsplit(s,">")[[1]][1],"<")[[1]][2],",")[[1]],":")
  attributes=unlist(lapply(a,function(x)x[[1]]))
  attribute=attributes[[1]]

  ts = lapply(a,function(x)x[[2]])
  nullable = rep(FALSE,length(ts))
  n = grep("null",ts,ignore.case=TRUE)
  if(any(n)) nullable[n]=TRUE

  types = gsub(" .*","",ts)
  type = types[1]

  d = gsub("\\]","",strsplit(s,"\\[")[[1]][[2]])
  d = strsplit(strsplit(d,"=")[[1]],",")
  dname = unlist(lapply(d[-length(d)],function(x)x[[length(x)]]))
  dtype = rep("int64",length(dname))
  nid = grepl("\\(",dname)
  if(any(nid))
  {
    dtype[nid] = gsub(".*\\((.*)\\)","\\1",dname[nid])
    dname[nid] = gsub("\\(.*","",dname[nid])
  }
  chunk_interval = unlist(lapply(d[-1],function(x)x[[2]]))
  chunk_overlap = unlist(lapply(d[-1],function(x)x[[3]]))
  d = lapply(d[-1],function(x)x[[1]])
  nid = !grepl(":",d)
  if(any(nid))
    d[nid] = paste("1:",d[nid],sep="")
  dlength = unlist(lapply(d,function(x)diff(as.numeric(strsplit(x,":")[[1]]))+1))

  D = list(name=dname,
           type=dtype,
           length=dlength,
           chunk_interval=chunk_interval,
           chunk_overlap=chunk_overlap,
           low=rep(NA,length(dname)),
           high=rep(NA,length(dname))
           )

  obj = new("scidb",
            call=match.call(),
            name=expr,
            attribute=attribute,
            type=type,
            attributes=attributes,
            types=types,
            nullable=nullable,
            D=D,
            dim=D$length,
            gc=new.env(),
            length=prod(D$length)
        )
  obj
}
