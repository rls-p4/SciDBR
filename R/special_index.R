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
  ans = scidb(query)  # make working query into a scidb object
  xdims = dimensions(ans)
  xstart = scidb_coordinate_start(ans)
  xend = scidb_coordinate_end(ans)

  for(j in 1:length(idx))
  {
    if(idx[[j]])
    {
# Check for and try to adjust this akward case:
      if(is.scidbdf(i[[j]])) i[[j]] = project(i[[j]],1)
      if(is.numeric(i[[j]]))
      {
# Special index case 1: non-contiguous numeric indices
        MASK = redimension(bind(as.scidb(as.double(i[[j]])),"index","int64(val)"),schema=sprintf("<i:int64>%s",build_dim_schema(ans,I=j,newnames="index",newend="*")))
      } else if(is.character(i[[j]]))
      {
        stop("dimension labels are not supported")
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
        if(scidb_coordinate_start(MASK)[1] != xstart[j])
        {
# oh no
          MASK = redimension(MASK, sprintf("%s%s", build_attr_schema(MASK), build_dim_schema(MASK, newend="*")))
          MASK = reshape(MASK, sprintf("%s%s", build_attr_schema(MASK), build_dim_schema(MASK, newstart=xstart[j])))
        }
      }
      MASK = scidbeval(MASK)  # Note! Can't use temp array because the output will depend on this!
# -- now, cross_join with MASK and adjust for redim=TRUE case if required
      ans = project(merge(ans, MASK, by.x=xdims[j], by.y=dimensions(MASK)), 1:length(ans@attributes))
      if(redim)  # this means densify, it's tricky
      {
        newdim = project(bind(cumulate(bind(apply(ans,j,count),"_","int64(1)"), "sum(_) as __"),"_","__ - 1"),"_")
        newdim = scidbeval(newdim)
        newstart = scidb_coordinate_start(ans)
        newend = scidb_coordinate_end(ans)
        newnames = xdims
        newstart[j] = "0"
        newend[j] = noE(max(newdim))
        newnames[j] = "_"
        ans = dimension_rename(redimension(merge(ans,newdim), schema=sprintf("%s%s",build_attr_schema(ans), build_dim_schema(ans,newstart=newstart, newend=newend, newnames=newnames))), old="_", new=xdims[j])
      }
    }
  } # end for loop over axes
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
