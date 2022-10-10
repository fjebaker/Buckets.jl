# BinningAlgorithms

Work in progress...

Currently only exports single function 
```
help?> bincontiguous
search: bincontiguous

  bincontiguous(X, y, bins; kwargs...)

  Bin data in y by X into bins, that is to say, reduce the y data corresponding to coordinates X over
  domain ranges given by bins.

  The contiguous requirement here is that bins describes the bin edges, such that the minimal value of bin
  i is the maximal value of bin (i-1). This function will bin all y with X < minimum(bins) into the first
  bin, and all y with X > maximum(bins) into the last bin.

  This function, and its dispatches, accept the following keyword arguments

    •  reduction=sum: a statistical function used to reduce all y in a given bin.

  ─────────────────────────────────────────────────────────────────────────────────────────────────────────

  bincontiguous(X1, X2, y, bins1, bins2; kwargs...)

  Two dimensional contiguous binning, where y can either be

    •  AbstractMatrix: in this case, X1 and X2 are assumed to be the columns and rows respectively of
       the data in y,

  and bins1 (bins2) the bin edges for X1 (X2).

    •  AbstractVector: X1 and X2 are effectively the coordinates of y
```