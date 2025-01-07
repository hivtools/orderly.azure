test_that("can read a file from a file share", {
  tmp <- withr::local_tempfile()
  writeLines("hello", tmp)
  fs <- create_mock_fs(download = mockery::mock(tmp))
  expect_equal(fs_read_path(fs$fs, "path", readLines), "hello")
  mockery::expect_called(fs$fs$download, 1)
  expect_equal(mockery::mock_args(fs$fs$download)[[1]], list("path"))
  expect_false(file.exists(tmp))
})


test_that("can list packets when empty", {
  fs <- create_mock_fs(
    download = mockery::mock(stop("some error")),
    list = mockery::mock(list(name = character())))
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  res <- driver$list()
  expect_equal(res, data_frame(packet = character(),
                               time = numeric(),
                               hash = character()))
  mockery::expect_called(fs$fs$download, 1)
  expect_equal(mockery::mock_args(fs$fs$download)[[1]],
               list("known.json"))
  mockery::expect_called(fs$fs$list, 1)
  expect_equal(mockery::mock_args(fs$fs$list)[[1]], list("known"))
})


test_that("can list known packets", {
  meta1 <- withr::local_tempfile()
  meta2 <- withr::local_tempfile()
  meta <- data_frame(
    packet = c("20230904-163829-d8f95215", "20230904-164221-ecbe3fcc"),
    time = c(1693845514.5003, 1693845742.282),
    hash = c("sha256:ab5a58831", "sha256:82b259b39"))
  writeLines(jsonlite::toJSON(as.list(meta[1, ]), auto_unbox = TRUE), meta1)
  writeLines(jsonlite::toJSON(as.list(meta[2, ]), auto_unbox = TRUE), meta2)
  fs <- create_mock_fs(
    download = mockery::mock(stop("some error"), meta1, meta2),
    list = mockery::mock(list(name = meta$packet)),
    upload = mockery::mock())
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  res <- driver$list()
  expect_equal(res$packet, meta$packet)
  expect_equal(res$time, meta$time)
  expect_equal(res$hash, meta$hash)

  mockery::expect_called(fs$fs$download, 3)
  expect_equal(mockery::mock_args(fs$fs$download),
               list(list("known.json"),
                    list(file.path("known", meta$packet[[1]])),
                    list(file.path("known", meta$packet[[2]]))))

  mockery::expect_called(fs$fs$list, 1)
  expect_equal(mockery::mock_args(fs$fs$list)[[1]],
               list("known"))
  mockery::expect_called(fs$fs$upload, 1)
})


test_that("only check previously unknown packets when listing", {
  meta1 <- withr::local_tempfile()
  meta2 <- withr::local_tempfile()
  known <- withr::local_tempfile()
  meta <- data_frame(
    packet = c("20230904-163829-d8f95215", "20230904-164221-ecbe3fcc"),
    time = c(1693845514.5003, 1693845742.282),
    hash = c("sha256:ab5a58831", "sha256:82b259b39"))
  writeLines(jsonlite::toJSON(meta[1, ]), known)
  writeLines(jsonlite::toJSON(as.list(meta[1, ]), auto_unbox = TRUE), meta1)
  writeLines(jsonlite::toJSON(as.list(meta[2, ]), auto_unbox = TRUE), meta2)
  fs <- create_mock_fs(
    download = mockery::mock(known, meta2),
    list = mockery::mock(list(name = meta$packet)),
    upload = mockery::mock())
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  res <- driver$list()
  expect_equal(res$packet, meta$packet)
  expect_equal(res$time, meta$time)
  expect_equal(res$hash, meta$hash)

  mockery::expect_called(fs$fs$download, 2)
  expect_equal(mockery::mock_args(fs$fs$download),
               list(list("known.json"),
                    list(file.path("known", meta$packet[[2]]))))
  mockery::expect_called(fs$fs$list, 1)
  expect_equal(mockery::mock_args(fs$fs$list)[[1]],
               list("known"))
  mockery::expect_called(fs$fs$upload, 1)
})


test_that("simpler call when all known", {
  meta1 <- withr::local_tempfile()
  meta2 <- withr::local_tempfile()
  known <- withr::local_tempfile()
  meta <- data_frame(
    packet = c("20230904-163829-d8f95215", "20230904-164221-ecbe3fcc"),
    time = c(1693845514.5003, 1693845742.282),
    hash = c("sha256:ab5a58831", "sha256:82b259b39"))
  writeLines(jsonlite::toJSON(meta), known)
  writeLines(jsonlite::toJSON(as.list(meta[1, ]), auto_unbox = TRUE), meta1)
  writeLines(jsonlite::toJSON(as.list(meta[2, ]), auto_unbox = TRUE), meta2)
  fs <- create_mock_fs(
    download = mockery::mock(known, meta2),
    list = mockery::mock(list(name = meta$packet)),
    upload = mockery::mock())
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  res <- driver$list()
  expect_equal(res$packet, meta$packet)
  expect_equal(res$time, meta$time)
  expect_equal(res$hash, meta$hash)

  mockery::expect_called(fs$fs$download, 1)
  expect_equal(mockery::mock_args(fs$fs$download)[[1]],
               list("known.json"))
  mockery::expect_called(fs$fs$list, 1)
  expect_equal(mockery::mock_args(fs$fs$list)[[1]],
               list("known"))
  mockery::expect_called(fs$fs$upload, 0)
})


test_that("can read metadata", {
  meta1 <- withr::local_tempfile()
  meta2 <- withr::local_tempfile()
  meta3 <- withr::local_tempfile()
  writeLines("meta1", meta1)
  writeLines("meta2", meta2)
  writeLines("meta3", meta3)

  fs <- create_mock_fs(
    download = mockery::mock(meta1, meta2, meta3))

  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)

  expect_equal(driver$metadata(character()), character())
  mockery::expect_called(fs$fs$download, 0)

  expect_equal(driver$metadata("id1"), "meta1")
  mockery::expect_called(fs$fs$download, 1)
  expect_equal(mockery::mock_args(fs$fs$download)[[1]],
               list("metadata/id1"))

  expect_equal(driver$metadata(c("id2", "id3")), c("meta2", "meta3"))
  mockery::expect_called(fs$fs$download, 3)
  expect_equal(mockery::mock_args(fs$fs$download)[2:3],
               list(list("metadata/id2"), list("metadata/id3")))
})


test_that("can download a file", {
  fs <- create_mock_fs(
    download = mockery::mock())
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  driver$fetch_file("md5:abcdef0123", "dest")
  mockery::expect_called(fs$fs$download, 1)
  expect_equal(
    mockery::mock_args(fs$fs$download)[[1]],
    list("files/md5_abcdef0123", "dest"))
})


test_that("can list unknown packets", {
  fs <- create_mock_fs()
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  unlockBinding(quote("list"), driver)
  driver$list <- mockery::mock(c("a", "b", "c"), cycle = TRUE)
  expect_equal(driver$list_unknown_packets(c("a", "d")), "d")
  expect_equal(driver$list_unknown_packets(c("a")), character(0))
  expect_equal(driver$list_unknown_packets(character()), character(0))
  expect_equal(driver$list_unknown_packets(c("d", "e", "f")), c("d", "e", "f"))
  mockery::expect_called(driver$list, 4)
  expect_equal(mockery::mock_args(driver$list),
               rep(list(list()), 4))
})


test_that("can list unknown files", {
  fs <- create_mock_fs()
  hashes <- c("md5_abc", "md5_123", "md5_456")
  fs <- create_mock_fs(
    list = mockery::mock(data_frame(name = hashes), cycle = TRUE))
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  expect_equal(driver$list_unknown_files("md5:789"), "md5:789")
  expect_equal(driver$list_unknown_files(character()), character())
  expect_equal(
    driver$list_unknown_files(c("md5:abc", "md5:123", "md5:789", "md5:321")),
    c("md5:789", "md5:321"))
  mockery::expect_called(fs$fs$list, 3)
  expect_equal(mockery::mock_args(fs$fs$list),
               rep(list(list("files")), 3))
})


test_that("can push files", {
  fs <- create_mock_fs(
    upload = mockery::mock())
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)
  driver$push_file("/path/to/file", "md5:123")
  mockery::expect_called(fs$fs$upload, 1)
  expect_equal(mockery::mock_args(fs$fs$upload)[[1]],
               list("/path/to/file", "files/md5_123"))
})


test_that("can push metadata", {
  root <- suppressMessages(orderly2::orderly_example("default"))
  id <- suppressMessages(
    orderly2::orderly_run("data", echo = FALSE, root = root))
  path <- file.path(root, ".outpack", "metadata", id)
  hash <- orderly2:::hash_data(read_string(path), "sha256")

  fs <- create_mock_fs(
    upload = mockery::mock())
  driver <- orderly_location_azure$new(fs$url, fs$container, fs$key)

  ## Prevent cleanup of temporary files
  mock_tempfile <- mockery::mock(tempfile())
  mockery::stub(driver$push_metadata, "withr::local_tempfile", mock_tempfile)

  driver$push_metadata(id, hash, path)

  mockery::expect_called(fs$fs$upload, 2)
  args1 <- mockery::mock_args(fs$fs$upload)[[1]]
  expect_length(args1, 2)
  expect_type(args1[[1]], "character")
  expect_equal(readLines(args1[[1]], warn = FALSE), readLines(path, warn = FALSE))
  expect_equal(args1[[2]], file.path("metadata", id))

  args2 <- mockery::mock_args(fs$fs$upload)[[2]]
  expect_length(args2, 2)
  expect_s3_class(args2[[1]], "textConnection")
  expect_equal(args2[[2]], file.path("known", id))
  d <- jsonlite::fromJSON(readLines(args2[[1]]))
  expect_setequal(names(d), c("packet", "hash", "time"))
  expect_equal(d$packet, id)
  expect_equal(d$hash, hash)
  expect_type(d$time, "double")
})


test_that("create driver when exists already", {
  mock_fs <- list(download = mockery::mock(),
                  upload = mockery::mock(),
                  create = mockery::mock(),
                  list = mockery::mock(data_frame(name = character())))
  mock_client <- mockery::mock(mock_fs)

  mockery::stub(orderly_azure_fs, "fs_client", mock_client)
  res <- orderly_azure_fs("https://localhost", "container", "mykey")
  expect_identical(res, mock_fs)

  mockery::expect_called(mock_client, 1)
  expect_equal(mockery::mock_args(mock_client)[[1]],
               list("https://localhost", "container", "mykey"))

  mockery::expect_called(mock_fs$download, 1)
  expect_equal(mockery::mock_args(mock_fs$download)[[1]],
               list("orderly.azure"))

  mockery::expect_called(mock_fs$list, 0)
  mockery::expect_called(mock_fs$upload, 0)
  mockery::expect_called(mock_fs$create, 0)
})


test_that("create driver when new", {
  mock_fs <- list(download = mockery::mock(stop("does not exist")),
                  upload = mockery::mock(),
                  mkdir = mockery::mock(),
                  list = mockery::mock(data_frame(name = character())))
  mock_client <- mockery::mock(mock_fs)

  mockery::stub(orderly_azure_fs, "fs_client", mock_client)
  res <- orderly_azure_fs("https://localhost", "container", "mykey")
  expect_identical(res, mock_fs)

  mockery::expect_called(mock_client, 1)
  expect_equal(mockery::mock_args(mock_client)[[1]],
               list("https://localhost", "container", "mykey"))

  mockery::expect_called(mock_fs$download, 1)
  expect_equal(mockery::mock_args(mock_fs$download)[[1]],
               list("orderly.azure"))

  mockery::expect_called(mock_fs$list, 1)
  expect_equal(mockery::mock_args(mock_fs$list)[[1]], list())

  mockery::expect_called(mock_fs$upload, 1)

  mockery::expect_called(mock_fs$mkdir, 3)
  expect_equal(mockery::mock_args(mock_fs$mkdir),
               list(list("metadata"), list("files"), list("known")))
})


test_that("error if files exist at destination on creation", {
  mock_fs <- list(download = mockery::mock(stop("does not exist")),
                  upload = mockery::mock(),
                  create = mockery::mock(),
                  list = mockery::mock(data_frame(name = "x")))
  mock_client <- mockery::mock(mock_fs)

  mockery::stub(orderly_azure_fs, "fs_client", mock_client)
  expect_error(
    orderly_azure_fs("https://localhost", "123", "mykey"),
    "Container 123 cannot be used for orderly; contains other files")
})


test_that("create driver if not in cache", {
  fs <- create_mock_fs()

  mock_fs <- mockery::mock(fs$fs)
  mockery::stub(orderly_azure_cached, "orderly_azure_fs", mock_fs)

  expect_identical(
    orderly_azure_cached(fs$url, fs$container, fs$key),
    fs$fs)
  mockery::expect_called(mock_fs, 0)

  cache[[fs$cache_key]] <- NULL

  expect_identical(
    orderly_azure_cached(fs$url, fs$container, fs$key),
    fs$fs)
  mockery::expect_called(mock_fs, 1)
  expect_equal(mockery::mock_args(mock_fs)[[1]],
               list(fs$url, fs$container, fs$key))
})
