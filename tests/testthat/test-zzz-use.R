test_that("can construct a orderly_location_azure object", {
  testthat::skip("Run this one manually - needs credentials")
  root <- list()
  for (nm in c("a", "b")) {
    root[[nm]] <- suppressMessages(orderly2::orderly_example("default"))
    args <- list(driver = "orderly.azure::orderly_location_azure",
                 url = "https://orderlydemo.file.core.windows.net/",
                 container = "demo",
                 key = "KEY")
    orderly2::orderly_location_add("sp", "custom", args, root = root[[nm]])
  }

  orderly2::orderly_location_fetch_metadata(root = root[[nm]])

  id <- suppressMessages(
    orderly2::orderly_run("data", root = root$a, echo = FALSE))
  orderly2::orderly_location_push(id, "sp", root = root$a)

  orderly2::orderly_location_pull_metadata(root = root$b)
  ids <- orderly2::orderly_search(NULL, allow_remote = TRUE,
                                  root = root$b)
  expect_true(id %in% ids)
  orderly2::orderly_location_pull(id, root = root$b)
  expect_true(file.exists(file.path(root$b, "archive", "data", id)))
})
