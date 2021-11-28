#!/bin/bash

mkdir RawData
rm -r RawData/*
find uploads -type f -name '*.zip' -exec unzip {} -d RawData \;
find uploads -type f -name '*.gz' -exec tar xf {} -C RawData \;
find RawData -type f -name '*.gz' -exec tar xf {} -C RawData \;
# find RawData -mindepth 2 -type f -exec mv -t RawData -i '{}' +
find RawData -mindepth 2 -type f -exec mv -i '{}' RawData ';'
rm -r RawData/*/
