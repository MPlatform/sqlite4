# 2013 Feb 20
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#
# This file contains common code used the various lsm tests in this
# directory.
#


proc db_fetch {db key} {
  db csr_open csr
  csr seek $key eq
  set ret [csr value]
  csr close
  set ret
}


