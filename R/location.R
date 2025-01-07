##' @name orderly_location_azure
##' @rdname orderly_location_azure
##' @title Orderly Location for Azure file storage
##'
##' @description A driver to put orderly2 archives on
##'   Azure. Individual methods are not documented here as this
##'   just needs to satisfy the interface required by orderly2
##'   and is not for direct user use. See
##'   [orderly2::orderly_location_add] for details.
##'
##' @export
orderly_location_azure <- R6::R6Class(
  "orderly_location_azure",

  private = list(
    ## The 'azure_fs_client' object returned
    client = NULL
  ),

  public = list(
    ##' @param url The base url of your Azure file share, such
    ##' as `https://storename.file.core.windows.net/`
    ##' (the `https://` prefix is required)
    ##'
    ##' @param container Name of the storage container on Azure i.e. name of
    ##' the file share
    ##'
    ##' @param ... Additional args for authenticating with Azure, passed
    ##' straight through to [AzureStor::storage_endpoint]
    initialize = function(url, container, ...) {
      private$client <- orderly_azure_cached(url, container, ...)
    },

    verify = function() {
      ## No verification needed, we verify when creating the
      ## Azure file store object
    },

    list = function() {
      known <- tryCatch(
        fs_read_path(private$client, "known.json",
                     function(path) jsonlite::read_json(path, TRUE)),
        error = function(e) {
          data_frame(packet = character(),
                     time = numeric(),
                     hash = character())
        })
      new <- setdiff(private$client$list("known")$name,
                     known$packet)
      if (length(new) > 0) {
        dat <- lapply(file.path("known", new), function(path_id) {
          fs_read_path(private$client, path_id, jsonlite::read_json)
        })
        new <- data_frame(packet = vcapply(dat, "[[", "packet"),
                          time = vnapply(dat, "[[", "time"),
                          hash = vcapply(dat, "[[", "hash"))
        known <- rbind(known, new)
        rownames(known) <- NULL
        con <- textConnection(jsonlite::toJSON(known))
        private$client$upload(con, "known.json")
      }
      known
    },

    metadata = function(packet_ids) {
      vcapply(file.path("metadata", packet_ids), function(path_id) {
        fs_read_path(private$client, path_id, read_string)
      }, USE.NAMES = FALSE)
    },

    fetch_file = function(hash, dest) {
      src <- file.path("files", orderly_to_azure_name(hash))
      private$client$download(src, dest)
    },

    list_unknown_packets = function(ids) {
      setdiff(ids, self$list())
    },

    list_unknown_files = function(hashes) {
      known <- azure_to_orderly_name(private$client$list("files")$name)
      setdiff(hashes, known)
    },

    push_file = function(src, hash) {
      dest <- file.path("files", orderly_to_azure_name(hash))
      private$client$upload(src, dest)
    },

    push_metadata = function(packet_id, hash, path) {
      orderly2:::hash_validate_data(read_string(path), hash)
      private$client$upload(path, file.path("metadata", packet_id))
      dat <- jsonlite::toJSON(
        list(packet = jsonlite::unbox(packet_id),
             time = jsonlite::unbox(as.numeric(Sys.time())),
             hash = jsonlite::unbox(hash)))
      con <- textConnection(dat)
      private$client$upload(con, file.path("known", packet_id))
    }
  )
)

orderly_azure_fs <- function(url, container_name, ...) {
  fs <- fs_client(url, container_name, ...)

  path_test <- "orderly.azure"
  exists <- tryCatch({
    fs$download(path_test)
    TRUE
  }, error = function(e) FALSE)
  if (exists) {
    return(fs)
  }
  if (nrow(fs$list()) > 0L) {
    stop(sprintf(
      "Container %s cannot be used for orderly; contains other files",
      container_name))
  }
  con <- textConnection("orderly.azure")
  fs$upload(con, path_test)
  fs$mkdir("metadata")
  fs$mkdir("files")
  fs$mkdir("known")
  fs
}

fs_read_path = function(fs, path, read) {
  dest <- fs$download(path)
  on.exit(unlink(dest))
  read(dest)
}

## We can't upload files to Azure with a : in the name
## we want to use the hash for the filename as
## <algorithm>:<hash> so convert : into _
## and convert back on the way out
orderly_to_azure_name <- function(orderly_name) {
  sub(":", "_", orderly_name)
}

azure_to_orderly_name <- function(azure_name) {
  sub("_", ":", azure_name)
}

cache <- new.env(parent = emptyenv())
orderly_azure_cached <- function(url, container_name, ...) {
  cache_key <- paste(url, container_name)
  if (is.null(cache[[cache_key]])) {
    cache[[cache_key]] <- orderly_azure_fs(url, container_name, ...)
  }
  cache[[cache_key]]
}
