#!/bin/sh

echo "m4_define([VERSION_NUMBER],[$(cat VERSION)])" > VERSION.m4
autoscan && autoheader && autoconf
