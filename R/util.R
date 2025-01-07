`%||%` <- function(x, y) { # nolint
  if (is.null(x)) y else x
}


read_string <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
}


vcapply <- function(X, FUN, ...) { # nolint
  vapply(X, FUN, "", ...)
}


vnapply <- function(X, FUN, ...) { # nolint
  vapply(X, FUN, numeric(1), ...)
}


data_frame <- function(...) {
  data.frame(..., stringsAsFactors = FALSE)
}
