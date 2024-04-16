#!/bin/bash

export google_credentials_dir=$(dirname "$TF_VAR_google_credentials")
export google_credentials_file=$(basename "$TF_VAR_google_credentials")
