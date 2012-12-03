#!/bin/sh
# \
exec tclsh "$0" "$@"

package require sqlite3

##########################################################################
# Procedures used when running tests (collecting data).
#
proc exec_lsmtest_speed {nSec spec} {
  set fd [open [list |./lsmtest speed2 {*}$spec]]
  set res [list]

  puts -nonewline "./lsmtest speed2"
  foreach s $spec {
    if {[llength $s]==1} {
      puts -nonewline " $s"
    } else {
      puts -nonewline " \"$s\""
    }
  }
  puts ""

  set initial [clock seconds]

  while {![eof $fd] && ($nSec==0 || ([clock second]-$initial)<$nSec)} { 
    set line [gets $fd]
    set sz 0
    catch { set sz [file size testdb.lsm] }
    puts "$line ([expr $sz/1024])"
    if {[string range $line 0 0]=="#"} continue
    if {[llength $line]==0} continue
    lappend res $line
  }
  catch { close $fd }
  set res
}

proc run_speed_test {zDb nTimeout nWrite nFetch nPause nRepeat zSystem zName} {

  set spec [list -w $nWrite -f $nFetch -r $nRepeat -p $nPause -system $zSystem]
  set res [exec_lsmtest_speed $nTimeout $spec]
  set cmd "lsmtest speed2 -w $nWrite -f $nFetch -r $nRepeat -p $nPause" 
  append cmd " -system \"$zSystem\""

  sqlite3 db $zDb

  db eval {
    PRAGMA synchronous = OFF;
    CREATE TABLE IF NOT EXISTS run(
      runid INTEGER PRIMARY KEY, nWrite INT, nFetch INT, nPause INT, 
      cmd TEXT, name TEXT
    );
    CREATE TABLE IF NOT EXISTS sample(
      runid INT, sample INT, writems INT, fetchms INT, 
      PRIMARY KEY(runid, sample)
    );
    INSERT INTO run VALUES(NULL, $nWrite, $nFetch, $nPause, $cmd, $zName);
  }

  set id [db last_insert_rowid]
  foreach sample $res {
    foreach {a b c} $sample {}
    db eval { INSERT INTO sample VALUES($id, $a, $b, $c) }
  }

  db close
}
#
# End of procs used while gathering data.
##########################################################################

##########################################################################

proc chart_y_to_canvas {y} {
  foreach v [uplevel {info vars c_*}] { upvar $v $v }
  set ytotal [expr ($c_height - $c_top_margin - $c_bottom_margin)]
  expr $c_height - $c_bottom_margin - int($ytotal * $y / $c_ymax)
}
proc chart_sample_to_canvas {nSample x} {
  foreach v [uplevel {info vars c_*}] { upvar $v $v }
  set xtotal [expr ($c_width - $c_left_margin - $c_right_margin)]
  expr {$c_left_margin + ($x*$xtotal/$nSample)}
}

proc draw_line_series {nSample lSeries tag} {
  foreach v [uplevel {info vars c_*}] { upvar $v $v }
  set xtotal [expr ($c_width - $c_left_margin - $c_right_margin)]
  set ytotal [expr ($c_height - $c_top_margin - $c_bottom_margin)]

  set x 0
  for {set i 1} {$i < $nSample} {incr i} {
    set x2 [expr $x + int( double($xtotal - $x) / ($nSample-$i) )]
    if {[lindex $lSeries $i-1] < $c_ymax && [lindex $lSeries $i] < $c_ymax} {

      set y1 [chart_y_to_canvas [lindex $lSeries $i-1]]
      set y2 [chart_y_to_canvas [lindex $lSeries $i]]
      .c create line \
        [expr ($c_left_margin + $x)]  $y1 \
        [expr ($c_left_margin + $x2)] $y2 \
        -tag $tag
    }

    set x $x2
  }
}

proc draw_points_series {nSample lSeries tag} {
  foreach v [uplevel {info vars c_*}] { upvar $v $v }
  set xtotal [expr ($c_width - $c_left_margin - $c_right_margin)]
  set ytotal [expr ($c_height - $c_top_margin - $c_bottom_margin)]

  for {set i 0} {$i < $nSample} {incr i} {
    set x [chart_sample_to_canvas $nSample $i]
    set y [chart_y_to_canvas [lindex $lSeries $i]]
    .c create rectangle $x $y [expr $x+2] [expr $y+2] -tag $tag
  }
}

proc draw_bars_series {nSample nShift lSeries tag} {
  foreach v [uplevel {info vars c_*}] { upvar $v $v }
  set xtotal [expr ($c_width - $c_left_margin - $c_right_margin)]
  set ytotal [expr ($c_height - $c_top_margin - $c_bottom_margin)]

  set b [expr $c_height - $c_bottom_margin]
  for {set i 0} {$i < $nSample} {incr i} {
    set x [expr [chart_sample_to_canvas $nSample $i] + $nShift]
    set y [chart_y_to_canvas [lindex $lSeries $i]]
    .c create rectangle $x $y [expr $x+$c_bar_width-1] $b -tag $tag
  }
}

proc draw_text {iRun iRun2} {
  foreach v [uplevel {info vars c_*}] { upvar $v $v }

  set y $c_height
  if {$iRun2!=""} {
    array set metrics [font metrics {-size 8}]
    set cmd [db one {SELECT cmd FROM run WHERE runid=$iRun2}]
    .c create text 10 $y -anchor sw -text $cmd -fill grey70 -font {-size 8}
    set y [expr $y-$metrics(-ascent)]
  }
  set cmd [db one {SELECT cmd FROM run WHERE runid=$iRun}]
  .c create text 10 $y -anchor sw -text $cmd -fill grey70 -font {-size 8}
}

proc format_integer {n} {
  if { ($n % 1000000)==0 } { return "[expr $n/1000000]M" }
  if { $n>1000000 && ($n % 100000)==0 } { 
    return "[expr double($n)/1000000]M" 
  }
  if { ($n % 1000)==0 } { return "[expr $n/1000]K" }
  return $n
}

proc populate_chart {nSample nShift iRun colors} {
  foreach v [uplevel {info vars c_*}] { upvar $v $v }
  upvar nWrite nWrite
  upvar nFetch nFetch
  set name [db one "SELECT name FROM run WHERE runid=$iRun"]

  set lWrite [list]
  set lFetch [list]
  for {set i 0} {$i < $nSample} {incr i} {
    set q "SELECT writems, fetchms FROM sample WHERE runid=$iRun AND sample=$i"
    db eval $q break
    lappend lWrite [expr {(1000.0*$nWrite) / $writems}]
    lappend lFetch [expr {(1000.0*$nFetch) / $fetchms}]
  }

  draw_bars_series   $nSample $nShift $lWrite writes_p_$iRun
  draw_points_series $nSample $lFetch fetches_p_$iRun

  set lWrite [list]
  set lFetch [list]
  for {set i 0} {$i < $nSample} {incr i} {
    set q    "SELECT 1000.0 * ($i+1) * $nWrite / sum(writems) AS r1, "
    append q "       1000.0 * ($i+1) * $nFetch / sum(fetchms) AS r2  "
    append q "FROM sample WHERE runid=$iRun AND sample<=$i"

    db eval $q break
    lappend lWrite $r1
    lappend lFetch $r2
  }
  
  draw_line_series $nSample $lWrite writes_$iRun
  draw_line_series $nSample $lFetch fetches_$iRun

  # Create the legend for the data drawn above.
  #
  array set metrics [font metrics default]
  set y $c_legend_ypadding
  set x [expr $c_width - $c_legend_padding]
  set xsym [expr { $c_width 
     - [font measure default "$name cumulative fetches/sec"]
  }]

  foreach {t g} {
    "$name fetches/sec" {
      rectangle 0 0 -2 2 -tag fetches_p_$iRun
    }
    "$name writes/sec" {
      rectangle 0 0 $c_bar_width $metrics(-ascent) -tag writes_p_$iRun
    }
    "$name cumulative fetches/sec" {
      line 0 0 -30 0 -tag fetches_$iRun
    }
    "$name cumulative writes/sec"  {
      line 0 0 -30 0 -tag writes_$iRun
    }
  } {
    .c create text $x $y -tag legend_$iRun -text [subst $t] -anchor e

    set id [eval [concat {.c create} [subst $g]]]
    .c addtag legend_$iRun withtag $id

    set box [.c bbox $id]
    set xmove [expr {$xsym - ([lindex $box 0]/2 + [lindex $box 2]/2)}]
    set ymove [expr $y - ([lindex $box 1]/2 + [lindex $box 3]/2)]

    .c move $id $xmove $ymove
    incr y $metrics(-linespace)
  }

  .c itemconfigure writes_$iRun    -fill [lindex $colors 0] -width 2
  .c itemconfigure fetches_$iRun   -fill [lindex $colors 2] 
  .c itemconfigure writes_p_$iRun  -fill [lindex $colors 1] 
  .c itemconfigure fetches_p_$iRun -fill [lindex $colors 3]
  catch { .c itemconfigure fetches_p_$iRun -outline [lindex $colors 3] }
  catch { .c itemconfigure writes_p_$iRun  -outline [lindex $colors 1] }
}

proc generate_chart {png db iRun {iRun2 {}}} {
  sqlite3 db $db
  db eval { SELECT nWrite, nFetch FROM run WHERE runid=$iRun } {}
  if {0==[info exists nWrite]} {
    error "No such run in db $db: $iRun"
  }

  set c_left_margin    50
  set c_bottom_margin  60
  set c_top_margin     20
  set c_right_margin  400
  set c_width         1250
  set c_height        350

  set c_ymax          300000
  set c_ytick          50000
  set c_ticksize        5
  set c_nxtick          10
  set c_dbsize_padding  20
  set c_legend_padding  20
  set c_legend_ypadding 100
  set c_bar_width 2

  package require Tk
  canvas .c
  .c configure -width $c_width -height $c_height
  pack .c -fill both -expand 1

  # Make the background white
  .c configure -background white
  draw_text $iRun $iRun2

  # Draw the box for the chart
  #
  set y [expr $c_height - $c_bottom_margin]
  set x [expr $c_width - $c_right_margin]
  .c create rectangle $c_left_margin $c_top_margin $x $y

  # Draw the vertical ticks
  #
  set ytotal [expr ($c_height - $c_top_margin - $c_bottom_margin)]
  for {set y $c_ytick} {$y <= $c_ymax} {incr y $c_ytick} {

    # Calculate the canvas y coord at which to draw the tick
    set ypix [expr {$c_height - $c_bottom_margin - ($y * $ytotal) / $c_ymax}]

    set left_tick_x  $c_left_margin
    set right_tick_x [expr $c_width - $c_right_margin - $c_ticksize]
    foreach x [list $left_tick_x $right_tick_x] {
      .c create line $x $ypix [expr $x+$c_ticksize] $ypix
    }

    .c create text $c_left_margin $ypix -anchor e -text "[format_integer $y]  "
    set x [expr $c_width - $c_right_margin]
    .c create text $x $ypix -anchor w -text "  [format_integer $y]"
  }

  # Figure out the total number of samples for this chart
  #
  set nSample [db one {SELECT count(*) FROM sample where runid=$iRun}]
  if {$nSample==0} { error "No such run: $iRun" }
  #set nSample 100
  
  # Draw the horizontal ticks
  #
  set w [expr {$c_width - $c_left_margin - $c_right_margin}]
  set xincr [expr ($nSample * $nWrite) / $c_nxtick]
  for {set i 1} {$i <= $c_nxtick} {incr i} {
    set x [expr $i * $xincr]
    set xpix [expr {$c_left_margin + ($w / $c_nxtick) * $i}]

    set b [expr $c_height-$c_bottom_margin]
    .c create line $xpix $b $xpix [expr $b-$c_ticksize]
    .c create text $xpix $b -anchor n -text [format_integer $x]
  }

  set x [expr (($c_width-$c_right_margin-$c_left_margin) / 2) + $c_left_margin]
  set y [expr $c_height - $c_bottom_margin + $c_dbsize_padding]
  .c create text $x $y -anchor n -text "Database Size (number of entries)"

  populate_chart $nSample 0 $iRun {black grey55 black black}
  if {$iRun2 != ""} {
    #populate_chart $nSample 3 $iRun2 {royalblue skyblue royalblue royalblue}
     
    set s $c_bar_width
    populate_chart $nSample $s $iRun2 {royalblue lightsteelblue royalblue royalblue}
    set box [.c bbox legend_$iRun]
    set shift [expr $c_legend_padding + [lindex $box 3]-[lindex $box 1]]
    .c move legend_$iRun2 0 $shift
  }

  .c lower fetches_p_$iRun
  .c lower fetches_p_$iRun2
  .c lower writes_p_$iRun
  .c lower writes_p_$iRun2

  bind .c <q> exit
  bind . <q> exit
  db close
}

proc capture_photo {z} {
  package require Img
  set img [image create photo -format window -data .c]
  $img write $z -format GIF
}

#    "autoflush=1M multi_proc=0"
#    "autoflush=1M multi_proc=0 mt_mode=4 mt_min_ckpt=2M mt_max_ckpt=3M"
#    "autoflush=4M multi_proc=0 autocheckpoint=8M"
#    "autoflush=4M multi_proc=0 mt_mode=4"
proc run_all_tests {} {
  set nInsert 50000
  set nSelect 50000
  set nSleep  20000
  set nPause   2500

  #set nInsert 5000
  #set nSelect 5000
  #set nSleep  2000
  #set nPause   250

  set bFirst 0

  foreach {name config} {
    single-threaded "autoflush=1M multi_proc=0"
    multi-threaded  
        "autoflush=1M multi_proc=0 mt_mode=4 mt_min_ckpt=2M mt_max_ckpt=3M"
    single-threaded "autoflush=4M multi_proc=0 autocheckpoint=8M"
    multi-threaded  "autoflush=4M multi_proc=0 mt_mode=4"
  } {
    if {$bFirst!=0} {
      puts "sleeping 20 seconds..." ; after $nSleep
    }
    run_speed_test res.db 900 $nInsert $nSelect 0 200 $config $name
    set bFirst 1
  }

  # Tests with a 2.5 second delay.
  #
  foreach {name config} {
    single-threaded "autoflush=4M multi_proc=0 autocheckpoint=8M"
    multi-threaded  "autoflush=4M multi_proc=0 mt_mode=4"
  } {
    puts "sleeping 20 seconds..." ; after $nSleep
    run_speed_test res.db 900 $nInsert $nSelect $nPause 200 $config $name
  }
}

#run_all_tests

generate_chart png res.db 1 2
update
capture_photo lsmperf1.gif
destroy .c

generate_chart png res.db 3 4
update
capture_photo lsmperf2.gif
destroy .c

generate_chart png res.db 5 6
update
capture_photo lsmperf3.gif
destroy .c

exit


