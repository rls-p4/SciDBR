# Auxillary merge functions for each special case follow. The main function
# appears at the bottom of this file.

# Special case 1: full set cross product
merge_scidb_cross = function(x, y)
{
# New attribute schema for y that won't conflict with x:
  newas = build_attr_schema(y, newnames=make.unique_(scidb_attributes(x), scidb_attributes(y)))
# Impose a reasonable chunk size for dense arrays
  chunky = scidb_coordinate_chunksize(y)
  chunkx   = scidb_coordinate_chunksize(x)
  chunk_elements = prod(c(as.numeric(chunky), as.numeric(chunkx)))
# Only compute these counts if we need to
  pdx = prod(dim(x))
  if(is.scidb(x)) pdx = dim(x)[1]
  pdy = prod(dim(y))
  if(is.scidb(y)) pdy = dim(y)[1]
  if(chunk_elements>1e6 && pdx==count(x) && pdy==count(y))
    {
      NC = length(chunkx) + length(chunky)
      NS = 1e6^(1/NC)
      chunky = rep(noE(NS), length(chunky))
      chunkx = rep(noE(NS), length(chunkx))
      x = repart(x, sprintf("%s%s", build_attr_schema(x), build_dim_schema(x, newchunk=chunkx)))
      y = repart(y, sprintf("%s%s", build_attr_schema(y), build_dim_schema(y, newchunk=chunkx)))
    }
  newds = build_dim_schema(y, newnames=make.unique_(dimensions(x), dimensions(y)))
  y = cast(y, sprintf("%s%s", newas, newds))
  query = sprintf("cross_join(%s, %s)", x@name, y@name)
  return(.scidbeval(query, FALSE, depend=list(x, y)))
}

# Special case 2: merge on attributes or combinations of attributes and dimensions
# x, y scidb objects
# by.x, by.y character vectors of dimension and/or attributes
merge_scidb_on_attributes = function(x, y, by.x, by.y, all.x, all.y)
{
  query = sprintf("equi_join(%s, %s, 'left_names=%s', 'right_names=%s', 'left_outer=%d', 'right_outer=%d')",
                  x@name, y@name, paste(by.x, collapse=","), paste(by.y, collapse=","), as.integer(all.x), as.integer(all.y))
  return(.scidbeval(query, FALSE, depend=list(x,y)))
}

# SciDB join, cross_join, and merge wrapper internal function
#
# x and y are SciDB array references
# `by` is either a single character indicating a dimension name common to both
#      arrays to join on, or a two-element list of character vectors of array
#      dimensions to join on.
# `fillin` is an optional argument specifying a value used to fill attributes
#          as required by merge, it defaults to null.
# `all` is an optional argument that, if TRUE, indicates full outer join;
# `all.x` and `all.y` also apply for partial left/right outer joins
# `equi_join` (optional) if TRUE always use the equi_join operator
#
`merge_scidb` = function(x, y, `by`, ...)
{
  if(length(dimensions(y)) > length(dimensions(x)))
  {
    z = y
    y = x
    x = z
  }
  mc = list(...)
  al = scidb_alias(x, y)
  by.x = by.y = NULL
  `all` = FALSE
  scidbmerge = FALSE
  fillin = "(null)"
  if(!is.null(mc$all)) `all` = mc$all
  all.x = `all`
  all.y = `all`
  equi_join = FALSE
  if(!is.null(mc$all.x)) all.x = mc$all.x
  if(!is.null(mc$all.y)) all.y = mc$all.y
  if(!is.null(mc$by.x)) by.x = mc$by.x
  if(!is.null(mc$by.y)) by.y = mc$by.y
  if(!is.null(mc$merge)) scidbmerge = mc$merge
  if(!is.null(mc$equi_join)) equi_join = mc$equi_join
  if(!is.null(mc$fillin)) fillin = sprintf("(%s)", mc$fillin)
  `eval` = FALSE
  xname = x@name
  yname = y@name

# Check input
  if(sum(!is.null(by.x), !is.null(by.y))==1)
  {
    stop("Either both or none of by.x and by.y must be specified.")
  }
  if((!is.null(by.x) && !is.null(by.y)))
  {
    `by` = NULL
  }

# Check for full cross case.
  if((is.null(`by`) && is.null(by.x) && is.null(by.y)) ||
      length(`by`)==0 && is.null(by.x) && is.null(by.y))
  {
    if(scidbmerge) stop("SciDB merge not supported in this context")
    return(merge_scidb_cross(x, y))
  }

# Convert identically specified by into separate by.x by.y
  if(length(by)>0)
  {
    by.x = `by`
    by.y = `by`
  }

# Check for numeric `by` specification (dimension index)
  if(is.numeric(by.x)) by.x = dimensions(x)[by.x]
  if(is.numeric(by.y)) by.y = dimensions(y)[by.y]

# Check for special join on attributes case
  if(equi_join || any(by.x %in% scidb_attributes(x)) || any(by.y %in% scidb_attributes(y)))
  {
    if(scidbmerge) stop("SciDB merge not supported in this context (only join)")
    return(merge_scidb_on_attributes(x, y, by.x, by.y, all.x, all.y))
  }

# OK, we've ruled out cross and attribute join special cases. We have left
# either the normal SciDB join/merge or cross_join on a subset of dimensions.

# New attribute schema for y that won't conflict with x:
  newas = build_attr_schema(y, newnames=make.unique_(scidb_attributes(x), scidb_attributes(y)))
# Check for join case (easy case)
  if((length(by.x) == length(by.y)) && all(dimensions(x) %in% by.x) && all(dimensions(y) %in% by.y))
  {
# Check for valid starting coordinates (they must be identical)
    if(!isTRUE(all.equal(scidb_coordinate_start(x), scidb_coordinate_start(y))))
    {
#      stop("Mis-matched starting coordinates") # used to error out, now try to redim
# try inserting a redimension
       xless = scidb_coordinate_start(x) < scidb_coordinate_start(y)
       yless = scidb_coordinate_start(y) < scidb_coordinate_start(x)
       if(any(xless))
       {
         newstart = scidb_coordinate_start(y)
         newstart[xless] = scidb_coordinate_start(x)
         y = redimension(y, schema=sprintf("%s%s", build_attr_schema(y), build_dim_schema(y, newstart=newstart)))
       }
       if(any(yless))
       {
         newstart = scidb_coordinate_start(x)
         newstart[yless] = scidb_coordinate_start(y)
         x = redimension(x, schema=sprintf("%s%s", build_attr_schema(x), build_dim_schema(x, newstart=newstart)))
       }
    }
# If the chunk sizes are identical, we're OK (join does not care about the
# upper array bounds). Otherwise we need redimension.
    if(!isTRUE(all.equal(scidb_coordinate_chunksize(x), scidb_coordinate_chunksize(y))))
    {
      newds = build_dim_schema(y, newnames=dimensions(x))
      castschema = sprintf("%s%s", newas, newds)
      reschema = sprintf("%s%s", newas, build_dim_schema(x))
      z = redimension(cast(y, castschema), reschema)
    } else
    {
      castschema = sprintf("%s%s", newas, build_dim_schema(y))
      z = cast(y, castschema)
    }
    if(all.x || all.y) # outer join
    {
      if(scidbmerge) stop("at most one of `all` and `merge` may be set TRUE")
      query = sprintf("equi_join(%s, %s, 'left_outer=%d', 'right_outer=%d')", x@name, z@name, as.integer(all.x), as.integer(all.y))
    }
    else
      if(scidbmerge)
      {
        query = sprintf("merge(%s, %s)", x@name, z@name)
      } else
      {
        query = sprintf("join(%s, %s)", x@name, z@name)
      }
    return(.scidbeval(query, eval, depend=list(x, y)))
  }


# Finally, the cross-join case (trickiest)
  if(scidbmerge) stop("cross-merge not yet supported")
# Cast and redimension y conformably with x along join dimensions:
  idx.x = which(dimensions(x) %in% by.x)
  msk.y = dimensions(y) %in% by.y
  newds = lapply(1:length(dimensions(y)),
    function(j) {
# It's possible to get a SciDB name conflict here (issue #41).
      y_dim = dimensions(y)[j]
      y_new = make.unique_(dimensions(x), y_dim)
      if(!msk.y[j])
      {
        build_dim_schema(y, I=j, bracket=FALSE, newnames=y_new)
      } else
      {
        ind = which(by.y %in% y_dim) # by index
        build_dim_schema(x, I=idx.x[ind], newnames=y_dim, bracket=FALSE, newend=scidb_coordinate_end(y)[j])
      }
    })
  newds = newds[!unlist(lapply(newds, is.null))]
  newds = sprintf("[%s]", paste(newds, collapse=","))

# If the chunk sizes are identical, we're OK (join does not care about the
# upper array bounds). Otherwise we need redimension. This is no longer needed
# after 14.12, but we keep the old optimization around for
# backward-compatibility.
  if(compare_versions(options("scidb.version")[[1]], 14.12))
  {
    castschema = sprintf("%s%s", newas, newds)
    z = cast(y, castschema)
  } else
  {
    if(isTRUE(compare_schema(x, y, ignore_attributes=TRUE, ignore_types=TRUE, ignore_nullable=TRUE, s1_dimension_index=idx.x, s2_dimension_index=which(msk.y), ignore_end=TRUE)))
    {
      castschema = sprintf("%s%s", newas, newds)
      z = cast(y, castschema)
    } else
    {
      reschema = sprintf("%s%s", newas, newds)
      castschema = sprintf("%s%s", newas, newds)
      z = redimension(cast(subarray(y, limits=reschema, between=TRUE), castschema), reschema)
    }
  }

# Join on dimensions.
  query = sprintf("cross_join(%s as %s, %s as %s", xname, al[1], z@name, al[2])
  k = min(length(by.x), length(by.y))
  by.x = by.x[1:k]
  by.y = by.y[1:k]
  cterms = unique(paste(c(al[1], al[2]), as.vector(rbind(by.x, by.y)), sep="."))
  cterms = paste(cterms, collapse=",")
  query  = paste(query, ",", cterms, ")", sep="")
  ans = .scidbeval(query, eval, depend=list(x, y))
  ans
}
