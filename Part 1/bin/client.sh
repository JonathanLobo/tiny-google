#!/bin/bash

TARGET_DIR=${TARGET_DIR:-./target}
java -cp "${TARGET_DIR}"/InvertedIndex-*.jar com.levandoski.invertedindex.service.SearchClient $*
