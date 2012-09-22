#!/bin/sh
# \
exec tclsh "$0" "$@"


proc exec_lsmtest_speed {nSec spec} {
  set fd [open [list |lsmtest speed2 {*}$spec]]
  set res [list]

  puts "lsmtest speed2 $spec"

  set initial [clock seconds]

  while {![eof $fd] && ($nSec==0 || ([clock second]-$initial)<$nSec)} { 
    set line [gets $fd]
    puts $line
    if {[string range $line 0 0]=="#"} continue
    if {[llength $line]==0} continue
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
    set terminal pngcairo size 1000,400
    $script
  "
  exec gnuplot << $script > $png 2>/dev/null
}

proc make_totalset {res nWrite} {
  set ret ""
  set nMs 0
  set nIns 0
  foreach row $res {
    foreach {i msInsert msFetch} $row {}
    incr nIns $nWrite
    incr nMs $msInsert
    append ret "$nIns [expr $nIns*1000.0/$nMs]\n"
  }
  append ret "end\n"
  set ret
}

proc make_dataset {res iRes nWrite nShift nOp} {
  set ret ""
  foreach row $res {
    set i [lindex $row 0]
    set j [lindex $row [expr $iRes+1]]
    set x [expr $i*$nWrite + $nShift]
    append ret "$x [expr int($nOp * 1000.0 / $j)]\n"
  }
  append ret "end\n"
  set ret
}

proc do_write_test {zPng nSec nWrite nFetch nRepeat lSys} {

  if {[llength $lSys]!=2 && [llength $lSys]!=4} {
    error "lSys must be a list of 2 or 4 elements"
  }

  set lRes [list]
  foreach {name sys} $lSys {
    set wt [list -w $nWrite -r $nRepeat -f $nFetch -system $sys]
    lappend lRes [exec_lsmtest_speed $nSec $wt]
    if {$sys != [lindex $lSys end]} {
      puts "Sleeping 20 seconds..."
      after 20000
    }
  }

  # Set up the header part of the gnuplot script.
  #
  set xmax 0
  foreach res $lRes {
    set xthis [expr [lindex $res end 0]*$nWrite + 5*$nWrite/4]
    if {$xthis>$xmax} {set xmax $xthis}
  }

  append labeltext "Test parameters:\\n"
  append labeltext "   $nWrite writes per iteration\\n"
  append labeltext "   $nFetch fetches per iteration\\n"
  append labeltext "   key size is 12 bytes\\n"
  append labeltext "   value size is 100 bytes\\n"
  set labelx [expr int($xmax * 1.2)]

  set nWrite2 [expr $nWrite/2]
  set y2setup ""
  if {$nFetch>0} {
    set y2setup {
      set ytics nomirror
      set y2tics nomirror
      set y2range [0:*]
    }
  }
  set script [subst -nocommands {
    set boxwidth $nWrite2
    set xlabel "Database Size"
    set y2label "Queries per second"
    set ylabel "Writes per second"
    set yrange [0:*]
    set xrange [0:$xmax]
    set key outside bottom
    $y2setup
    set label 1 "$labeltext" at screen 0.95,graph 1.0 right
  }]


  set cols [list {#B0C4DE #00008B} {#F08080 #8B0000}]
  set cols [lrange $cols 0 [expr ([llength $lSys]/2)-1]]

  set nShift [expr ($nWrite/2)]
  set plot1 ""
  set plot2 ""
  set plot3 ""
  set data1 ""
  set data2 ""
  set data3 ""

  foreach {name sys} $lSys res $lRes col $cols {
    foreach {c1 c2} $col {}

    if {$plot1 != ""} { set plot1 ", $plot1" }
    set plot1 "\"-\" ti \"$name writes/sec\" with boxes fs solid lc rgb \"$c1\"$plot1"
    set data1 "[make_dataset $res 0 $nWrite $nShift $nWrite] $data1"

    set plot3 ",\"-\" ti \"$name cumulative writes/sec\" with lines lc rgb \"$c2\" lw 2 $plot3"
    set data3 "[make_totalset $res $nWrite] $data3"

    if {$nFetch>0} {
      set new ", \"-\" ti \"$name fetches/sec\" axis x1y2 with points lw 3 lc rgb \"$c2\""
      set plot2 "$new $plot2"
      set data2 "[make_dataset $res 1 $nWrite $nWrite $nFetch] $data2"
    }

    incr nShift [expr $nWrite/4]
  }
  append script "plot $plot1 $plot2 $plot3\n"
  append script $data1
  append script $data2
  append script $data3

  append script "pause -1\n"
  exec_gnuplot_script $script $zPng
}

do_write_test x.png 60 25000 0 40 {
  lsm-st "mmap=1 multi_proc=0 safety=1"
  LevelDB leveldb
}
# lsm-mt     "mmap=1 multi_proc=0 safety=1 threads=3 autowork=0"
# lsm-st     "mmap=1 multi_proc=0 safety=1 threads=1 autowork=1"
# LevelDB leveldb
# SQLite sqlite3




