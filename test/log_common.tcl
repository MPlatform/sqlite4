# 2012 June 02
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



proc do_filesize_test {tn dbsz logsz} {
  uplevel [list do_test $tn { 
      list [file size test.db] [file size test.db-log] 
  } [list $dbsz $logsz]]
}

proc copy_db_files {from to} {
  forcecopy $from $to
  forcecopy $from-log $to-log
}

proc copy_and_recover {sql} {
  catch {db2 close}
  copy_db_files test.db test.db2
  sqlite4 db2 test.db2
  set res [execsql $sql db2]
  db2 close
  set res
}

proc do_recover_test {tn sql {res {}}} {
  set R [list]
  foreach r $res {lappend R $r}
  do_test $tn [list uplevel #0 [list copy_and_recover $sql]] $R
}
