# Handle three special indexing cases along each coordinate axis.
# 1. discontiguous numeric indices
# 2. character indices (via dimension arrays)
# 3. indexing by another array (join)
# All cases are converted to cross_join along each axis.  An attempt is
# maintained to propagate subsetting into nids, if they are present.
#
# x: A scidb/scidbdf object
# query: A query in process around x, we will add to this.
# i: A list of *all* user-requested subset selections (one for each axis)
# idx: A logical vector indicating the axes with special indexing
special_index = function(x, query, i, idx, eval=FALSE, drop=FALSE, redim=TRUE)
{
  xdims = dimensions(x)
  xstart = scidb_coordinate_start(x)
  xend = scidb_coordinate_end(x)
  xchunk = scidb_coordinate_chunksize(x)
  xoverlap = scidb_coordinate_overlap(x)

  ans = scidb(query)  # make working query into a scidb object
  new_dimnames = dimnames(x) # will be assigned to ans as required

  for(j in 1:length(idx))
  {
    if(idx[[j]])
    {
# Check for and try to adjust this akward case:
      if(is.scidbdf(i[[j]])) i[[j]] = project(i[[j]],1)
      if(is.numeric(i[[j]]))
      {
# Special index case 1: non-contiguous numeric indices
        MASK = redimension(bind(as.scidb(as.double(i[[j]])),"index","int64(val)"),schema=sprintf("<i:int64>%s",build_dim_schema(x,I=1,newnames="index",newend="*")))
      } else if(is.character(i[[j]]))
      {
# Case 2: character labels, consult a lookup array if possible
# XXX WRITE ME
      } else if(is.scidb(i[[j]]))
      {
# Case 3. A SciDB array, a densified cross_join selector.
# If it's Boolean, convert it to a sparse array for cross_join indexing
        MASK = i[[j]]
        if(length(MASK@attributes)>1) MASK=project(MASK,1)
        if(length(dimensions(MASK))>1) MASK=apply(MASK,1,count)
        if(scidb_types(MASK) == "bool")
        {
          MASK = MASK %==% TRUE
        }
        if(scidb_coordinate_start(MASK)[1] != scidb_coordinate_start(x)[j])
        {
# oh no
          MASK = redimension(MASK, sprintf("%s%s", build_attr_schema(MASK), build_dim_schema(MASK, newend="*")))
          MASK = reshape(MASK, sprintf("%s%s", build_attr_schema(MASK), build_dim_schema(MASK, newstart=scidb_coordinate_start(x)[j])))
        }
      }
# -- now, cross_join with MASK and adjust for redim=TRUE case if required
      ans = project(merge(ans, MASK, by.x=xdims[j], by.y=dimensions(MASK)), 1:length(ans@attributes))
      if(redim)  # this means densify, it's tricky
      {
        newdim = project(bind(cumulate(bind(apply(ans,1,count),"_","int64(1)"), "sum(_) as __"),"_","__ - 1"),"_")
        newstart = xstart
        newend = xend
        newnames = xdims
        newstart[j] = "0"
        newend[j] = noE(max(newdim))
        newnames[j] = "_"
        ans = dimension_rename(redimension(merge(ans,newdim), schema=sprintf("%s%s",build_attr_schema(ans), build_dim_schema(ans,newstart=newstart, newend=newend, newnames=newnames))), old="_", new=xdims[j])
      }
# Now handle the corresponding NID conformably. Gross.
      if(!is.null(dimnames(x)[[j]]))
      {
        new_dimnames[[j]] = project(merge(dimnames(x)[[j]], MASK, by.x=dimensions(dimnames(x)[[j]])[1], by.y=dimensions(MASK)), 1)
        if(redim)
        {
          new_dimnames[[j]] = dimension_rename(redimension(merge(dimnames(x)[[j]],newdim,by.x=dimensions(dimnames(x)[[j]]), by.y=dimensions(newdim)), schema=sprintf("%s%s",build_attr_schema(dimnames(x)[[j]]), build_dim_schema(dimnames(x)[[j]],newstart="0", newend=newend[j], newnames="_"))), old="_", new=dimensions(x)[[j]])
        }
      }
    }
  } # end for loop over axes
  dimnames(ans) = new_dimnames
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
