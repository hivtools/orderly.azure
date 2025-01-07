fs_client <- function(url, container_name, ...) {
  azure_fs_client$new(url, container_name, ...)
}

azure_fs_client <- R6::R6Class(
  "azure_fs_client",

  private = list(
    ## The azure file share object returned from AzureStor::storage_container
    fs = NULL
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
    initialize = function(url, container_name, ...) {
      storage <- AzureStor::storage_endpoint(url, ..., service = "file")
      private$fs <- tryCatch(
        AzureStor::storage_container(storage, container_name),
        error = function(e) {
          stop(sprintf(
            paste("Error locating storage container %s, check",
                  "it exists and you have permission - %s"),
            container_name, e$message), call. = FALSE)
        }
      )
    },

    list = function(dir = "/") {
      files <- AzureStor::list_storage_files(private$fs, dir = dir)
      ## Orderly expects to work with name as just the file name but
      ## AzureStor returns this with the folders appended, so remove them
      files$name <- basename(files$name)
      files
    },

    download = function(src, dest = tempfile()) {
      AzureStor::storage_download(private$fs, src, dest = dest)
      dest
    },

    upload = function(src, dest) {
      AzureStor::upload_azure_file(private$fs, src, dest)
    },

    mkdir = function(dirname) {
      AzureStor::create_storage_dir(fs, dirname)
    }
  )
)
