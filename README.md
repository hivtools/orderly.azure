# orderly.azure

[![R-CMD-check](https://github.com/hivtools/orderly.azure/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/hivtools/orderly.azure/actions/workflows/R-CMD-check.yaml)
[![experimental](http://badges.github.io/stability-badges/dist/experimental.svg)](http://github.com/badges/stability-badges)

[Orderly2](https://github.com/mrc-ide/orderly2) remote backed by an Azure file share.

To use this you will need
* Address of the file share which should be "https://<file-share-name>.file.core.windows.net/"
* The name of a container within the file share to store the remotes at, this must exist and be empty
* A way to authenticate, a key might be easiest but see `?AzureStor::storage_container`

## Installation

```R
# install.packages("remotes")
remotes::install_github("hivtools/orderly.azure")
```

## Usage

Add a remote

```R
args <- list(driver = "orderly.azure::orderly_location_azure",
             url = "https://<file-share-name>.file.core.windows.net/",
             container = "demo",
             key = "KEY")
orderly2::orderly_location_add("my-local-name", "custom", args)
```

It can then be used as normal, see [collaborative analysis vignette](https://mrc-ide.github.io/orderly2/articles/collaboration.html) from orderly2.

Note this is experimental, and shouldn't be relied upon yet. Better to use a file or http remote.
