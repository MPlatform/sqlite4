#!/bin/sh
# \
exec tclsh "$0" "$@"


proc exec_lsmtest_speed {nSec spec} {
  set fd [open [list |lsmtest speed2 {*}$spec]]
  set res [list]

  set initial [clock seconds]

  while {![eof $fd] && ($nSec==0 || ([clock second]-$initial)<$nSec)} { 
    set line [gets $fd]
    puts $line
    if {[string range $line 0 0]=="#"} continue
    lappend res $line
  }
  catch { close $fd }
  set res
}

proc write_to_file {zFile zScript} {
  set fd [open $zFile w]
  puts $fd $zScript
  close $fd
}

proc exec_gnuplot_script {script png} {
  write_to_file out "
    $script
    pause -1
  "
  
  set script "
    set terminal png
    $script
  "
  exec gnuplot << $script > $png 2>/dev/null
}

proc do_write_test {nSec nWrite nFetch nRepeat zSystem zPng} {
  set wt [list -w $nWrite -r $nRepeat -f $nFetch -system $zSystem]
  set res [exec_lsmtest_speed $nSec $wt]
  set script "set boxwidth [expr $nWrite/2]"
  append script {
    set xlabel "Rows Inserted"
    set y2label "Selects per second"
    set ylabel "Inserts per second"
    set yrange [0:*]
    set xrange [0:*]
    set xrange [0:*]
    set key box lw 0.01
  }

  if {$nFetch>0} {
    append script {
      set ytics nomirror
      set y2tics nomirror
      set y2range [0:*]
    }
  }

  append script {plot "-" ti "INSERT" with boxes fs solid lc rgb "#B0C4DE"}
  if {$nFetch>0} {
    append script {, "-" ti "SELECT" axis x1y2 with points lw 3 lc }
    append script {rgb "#000000"}
  }
  append script "\n"

  foreach row $res {
    foreach {i msInsert msFetch} $row {}
    set x [expr $i*$nWrite + $nWrite/2]
    append script "$x [expr int($nWrite * 1000.0 / $msInsert)]\n"
  }
  append script "end\n"

  if {$nFetch>0} {
    foreach row $res {
      foreach {i msInsert msFetch} $row {}
      set x [expr $i*$nWrite + $nWrite]
      append script "$x [expr int($nFetch * 1000.0 / $msFetch)]\n"
    }
    append script "end\n"
  }

  append script "pause -1\n"
  exec_gnuplot_script $script $zPng
}

do_write_test 60 100000 100000 100 "mmap=1 multi_proc=0 safety=1" x.png
after 10000
do_write_test 60 100000 100000 100 leveldb y.png




