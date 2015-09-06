# Handle three special indexing cases along each coordinate axis.
# 1. discontiguous numeric indices
# 2. character indices (NO LONGER SUPPORTED)
# 3. indexing by another array (join)
# All cases are converted to cross_join along each axis.
#
# x: A scidb/scidbdf object
# i: A list of *all* user-requested subset selections (one for each axis)
# idx: A logical vector indicating the axes with special indexing
# eval: Logical scidbeval array flag
# drop: TRUE means drop singleton dimensions
# redim: Set to FALSE to avoid redimension
# newstart,newend: possibly modified array coordinate start/end indices from
#    other indexing operations. only applies to !idx coordinates
#
# A big objective of this function is to defer redimensioning into a single
# statement at the end for efficiency.
special_index = function(x, i, idx, eval=FALSE, drop=FALSE, redim=TRUE, newstart, newend)
{
  newdim = dimensions(x)
  att = scidb_attributes(x)
  newschema = build_dim_schema(x, newstart=newstart, newend=newend)
  for(j in 1:length(idx))
  {
    if(idx[[j]])
    {
      eval = TRUE   # Avoids needing to keep around a bunch of ancillary arrays
      if(is.numeric(i[[j]]))
      {
# Special index case 1: non-contiguous numeric indices, somewhat easy case
        x = merge(x,as.scidb(i[[j]],dimension=TRUE),by.x=j,by.y=1)
        sa = scidb_attributes(x)
        newdim[j] = sa[length(sa)]  # Assign the new attribute for redim
      } else if(is.character(i[[j]]))
      {
# Special index case 2: character labels, no longer supported
        stop("dimension labels are not supported")
      } else if(is.scidb(i[[j]]) || is.scidbdf(i[[j]]))
      {
# Special index case 3: another SciDB array, tricky case
cat("YIKES\n")
      }
    }
  } # end for loop over axes
  if(redim)
  {
    x = redimension(x, schema=newschema, dim=newdim)
  } else
  {
    if(!all(scidb_attributes(x) %in% att)) x = project(x,att)
  }
  if(drop) x = drop_dim(x)
  if(`eval`) x = scidbeval(x)
  x
}
