special_index = function(x, query, i, idx, eval=FALSE, drop=FALSE, redim=TRUE)
{
  swap = NULL
  dependencies = x
  xdims = dimensions(x)
  xstart = scidb_coordinate_start(x)
  xend = scidb_coordinate_end(x)
  xchunk = scidb_coordinate_chunksize(x)
  xoverlap = scidb_coordinate_overlap(x)

  for(j in 1:length(idx))
  {
    N = xdims[j]
    dimlabel = make.unique_(xdims, sprintf("%s_1",N))
    if(is.list(swap[[1]]))
    {
      slabels = unlist(lapply(swap,function(x)x[[2]]))
      dimlabel = make.unique_(slabels, dimlabel)
    } else if(!is.null(swap))
    {
      dimlabel = make.unique_(swap[[2]], dimlabel)
    }
    if(idx[[j]])
    {
# Check for and try to adjust this akward case:
      if(is.scidbdf(i[[j]])) i[[j]] = project(i[[j]],1)
      if(is.numeric(i[[j]]))
      {
# Special index case 1: non-contiguous numeric indices
        tmp = data.frame(noE(unique(i[[j]])))
        if(nrow(tmp)!=length(i[[j]]))
        {
          warning("The scidb package doesn't support repeated indices in subarray selection")
        }
        names(tmp) = N
        i[[j]] = as.scidb(tmp, types="int64", dimlabel=dimlabel,
                        start=xstart[j],
                        chunkSize=xchunk[j],
                        rowOverlap=xoverlap[j], nullable=FALSE)
        swap = c(swap, list(list(old=N, new=dimlabel, length=length(tmp[,1]), start=xstart[j])))
        dependencies = c(dependencies, i[[j]])
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", i[[j]]@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as x, %s as y, x.%s, y.%s)",query, Q1, N, N)
      } else if(is.character(i[[j]]))
      {
# Case 2: character labels, consult a lookup array if possible
         lkup = replaceNA(x@gc$dimnames[[j]])
         if(length(dim(lkup))>1)
         {
# Hmmm. The lookup array has more than one dimension! Let's just use the first.
           lkup = slice(lkup,2:length(dim(lkup)),scidb_coordinate_start(lkup)[-1])
         }
         tmp = data.frame(i[[j]], stringsAsFactors=FALSE)
         names(tmp) = N
         tmp_1 = as.scidb(tmp, types="string", dimlabel=dimlabel,
                        start=xstart[j],
                        chunkSize=xchunk[j],
                        rowOverlap=xoverlap[j], nullable=FALSE)
        i[[j]] = attribute_rename(project(index_lookup(tmp_1, lkup, new_attr='_cazart'),"_cazart"),"_cazart",N)
        dependencies = c(dependencies, tmp_1)
        swap = c(swap, list(list(old=N, new=dimlabel, length=length(tmp[,1]), start=xstart[j])))
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", i[[j]]@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as x, %s as y, x.%s, y.%s)",query, Q1, N, N)
      } else if(is.scidb(i[[j]]))
      {
# Case 3. A SciDB array, a densified cross_join selector.
# If it's Boolean, convert it to a sparse array for cross_join indexing
        if(scidb_types(i[[j]])[[1]] == "bool")
        {
          i[[j]] = i[[j]] %==% TRUE
        }
        tmp = bind(i[[j]],N,dimensions(i[[j]])[1],eval=FALSE)
        tmp = sort(project(tmp, length(scidb_attributes(tmp)),eval=FALSE),eval=FALSE)
# scidb name conflict problems, check for and resolve them.
        tmp = attribute_rename(tmp, old=1, new=N)
        tmpaname = make.unique_(dimlabel, scidb_attributes(tmp))
        cst = paste(build_attr_schema(tmp,newnames=tmpaname),build_dim_schema(tmp,newnames=dimlabel))
        tmp = cast(tmp,cst,eval=FALSE)
        cnt = count(tmp)
        dependencies = c(dependencies, tmp)
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", tmp@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as x, %s as y, x.%s, y.%s)",query, Q1, N, N)
# Note start=0 comes from the sort...
        swap = c(swap, list(list(old=N, new=dimlabel, length=cnt, start=0)))
      }
    } else # No special index in this coordinate
    {
      len = scidb_coordinate_bounds(x)$length[[j]]
      if(!is.null(i[[j]])) len=length(i[[j]])
      st = 0
      if(!is.null(i[[j]])) st = i[[j]][1]
      swap = c(swap,list(list(old=N, new=N, length=len, start=st)))
    }
  }
  nn = sapply(swap, function(x) x[[2]])
  nl = sapply(swap, function(x) x[[3]])
  ns = sapply(swap, function(x) x[[4]])
# Propagate dimension labels
  new_dimnames = dimnames(x)
  if(!is.null(dimnames(x)))
  {
    new_dimnames =  vector(mode="list",length=length(dim(x)))
    for(j in 1:length(new_dimnames))
    {
      if(!is.null(dimnames(x)[[j]]))
      {
# crazy schema munging here!
        if(redim)
          new_dimnames[[j]] = scidb(sprintf("subarray(redimension(join(redimension(%s,<%s:int64>%s), %s), %s%s),null,null)",query,nn[j],build_dim_schema(x,I=j), dimnames(x)[[j]]@name, build_attr_schema(dimnames(x)[[j]]), build_dim_schema(x,I=j,newstart=ns[j],newnames=nn[j],newlen="*")))
        else
          new_dimnames[[j]] = scidb(sprintf("project(join(redimension(%s,<%s:int64>%s) as x, %s as y), y.%s)",query,nn[j],build_dim_schema(x,I=j), dimnames(x)[[j]]@name,scidb_attributes(dimnames(x)[[j]])[1]))
      }
    }
  }
  if(redim)
  {
    query = sprintf("subarray(cast(redimension(%s, %s%s),%s%s),%s)", query,
                    build_attr_schema(x),
                    build_dim_schema(x,newstart=ns,newnames=nn,newlen=nl),
                    build_attr_schema(x),
                    build_dim_schema(x,newstart=ns,newlen=nl),paste(rep('null',2*length(ns)),collapse=","))
  } else
  {
    query = sprintf("project(%s,%s)",query,scidb_attributes(x))
  }
  ans = .scidbeval(query, eval=FALSE, depend=dependencies)
  ans@gc$dimnames = new_dimnames
  if(drop)
  {
    ans = drop_dim(ans)
  }
  if(`eval`)
  {
    ans = scidbeval(ans)
  }
  ans
}
