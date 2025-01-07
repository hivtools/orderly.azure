clear_cache <- function() {
  rm(list = ls(cache), envir = cache)
}


create_mock_fs <- function(..., container = "mycontainer", key = "mykey") {
  url <- sprintf("https://%s.file.core.windows.net/", random_str())
  cache_key <- paste(url, container)
  fs <- list(...)
  cache[[cache_key]] <- fs
  list(url = url, container = container, fs = fs, key = key,
       cache_key = cache_key)
}


random_str <- function() {
  paste(sample(letters, 20, replace = TRUE), collapse = "")
}
