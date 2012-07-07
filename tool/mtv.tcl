
package require sqlite3
package require Tk

#############################################################################
# Code to set up scrollbars for widgets. This is generic, boring stuff.
#
namespace eval autoscroll {
  proc scrollable {widget path args} {
    ::ttk::frame $path
    set w  [$widget ${path}.widget {*}$args]
    set vs [::ttk::scrollbar ${path}.vs]
    set hs [::ttk::scrollbar ${path}.hs -orient horizontal]
    grid $w  -row 0 -column 0 -sticky nsew
  
    grid rowconfigure    $path 0 -weight 1
    grid columnconfigure $path 0 -weight 1
  
    set grid [list grid $vs -row 0 -column 1 -sticky nsew]
    $w configure -yscrollcommand [list ::autoscroll::scrollcommand $grid $vs]
    $vs configure -command       [list $w yview]
    set grid [list grid $hs -row 1 -column 0 -sticky nsew]
    $w configure -xscrollcommand [list ::autoscroll::scrollcommand $grid $hs]
    $hs configure -command       [list $w xview]
  
    return $w
  }
  proc scrollcommand {grid sb args} {
    $sb set {*}$args
    set isRequired [expr {[lindex $args 0] != 0.0 || [lindex $args 1] != 1.0}]
    if {$isRequired && ![winfo ismapped $sb]} {
      {*}$grid
    }
    if {!$isRequired && [winfo ismapped $sb]} {
      grid forget $sb
    }
  }
  namespace export scrollable
}
namespace import ::autoscroll::*
#############################################################################

proc populate_text_widget {db} {
  $::O(text) configure -state normal
  set id [lindex [$::O(tree) selection] 0]
  set frame [lindex $id end]

  set line [$db one {SELECT line FROM frame WHERE frame = $frame}]
  if {$line ne ""} {
    foreach {file line} [split $line :] {}
    set content [$db one "SELECT content FROM file WHERE name = '$file'"]
    $::O(text) delete 0.0 end

    set iLine 1
    foreach L [split $content "\n"] {
      if {$iLine == $line} {
        $::O(text) insert end "$L\n" highlight
      } else {
        $::O(text) insert end "$L\n"
      }
      incr iLine
    }
    $::O(text) yview -pickplace ${line}.0
  }
  $::O(text) configure -state disabled
}

proc populate_index {db} {
  $::O(text) configure -state normal
  
  $::O(text) delete 0.0 end
  $::O(text) insert end "\n\n"

  set L [format "    % -40s%12s%12s\n" "Test Case" "Allocations" "Bytes"]
  $::O(text) insert end $L
  $::O(text) insert end "    [string repeat - 64]\n"

  $db eval {
    -- SELECT 'TOTAL' AS ztest, sum(ncall) AS calls, sum(nbyte) AS bytes
    -- FROM malloc 
    -- UNION ALL

    SELECT ztest AS ztest, sum(ncall) AS calls, sum(nbyte) AS bytes
    FROM malloc 
    GROUP BY ztest

    ORDER BY 3 DESC
  } {
    set tags [list $ztest]
    if {$ztest eq $::O(current)} {
      lappend tags highlight
    }
    set L [format "    % -40s%12s%12s\n" $ztest $calls $bytes]
    $::O(text) insert end $L $tags

    $::O(text) tag bind $ztest <1> [list populate_tree_widget $db $ztest]
    $::O(text) tag bind $ztest <Enter> [list $::O(text) configure -cursor hand2]
    $::O(text) tag bind $ztest <Leave> [list $::O(text) configure -cursor ""]
  }

  $::O(text) configure -state disabled
}

proc sort_tree_compare {iLeft iRight} {
  global O
  switch -- [expr (int($O(tree_sort)/2))] {
    0 {
      set left  [$O(tree) item $iLeft -text]
      set right [$O(tree) item $iRight -text]
      set res [string compare $left $right]
    }
    1 {
      set left  [lindex [$O(tree) item $iLeft -values] 0]
      set right [lindex [$O(tree) item $iRight -values] 0]
      set res [expr $left - $right]
    }
    2 {
      set left  [lindex [$O(tree) item $iLeft -values] 1]
      set right [lindex [$O(tree) item $iRight -values] 1]
      set res [expr $left - $right]
    }
  }
  if {$O(tree_sort)&0x01} {
    set res [expr -1 * $res]
  }
  return $res
}

proc sort_tree {iMode} {
  global O
  if {$O(tree_sort) == $iMode} {
    incr O(tree_sort)
  } else {
    set O(tree_sort) $iMode
  }
  set T $O(tree)
  set items [$T children {}]
  set items [lsort -command sort_tree_compare $items]
  for {set ii 0} {$ii < [llength $items]} {incr ii} {
    $T move [lindex $items $ii] {} $ii
  }
}

proc trim_frames {stack} {
  while {[info exists ::O(ignore.[lindex $stack 0])]} {
    set stack [lrange $stack 1 end]
  }
  return $stack
}

proc populate_tree_widget {db zTest} {
  $::O(tree) delete [$::O(tree) children {}]

  for {set ii 0} {$ii < 15} {incr ii} {
    $db eval {
      SELECT 
        sum(ncall) AS calls, 
        sum(nbyte) AS bytes,
        trim_frames(lrange(lstack, 0, $ii)) AS stack
      FROM malloc
      WHERE (zTest = $zTest OR $zTest = 'TOTAL') AND llength(lstack)>$ii
      GROUP BY stack
      HAVING stack != ''
    } {
      set parent_id [lrange $stack 0 end-1]
      set frame [lindex $stack end]
      set line [$db one {SELECT line FROM frame WHERE frame = $frame}]
      set line [lindex [split $line /] end]
      set v [list $calls $bytes]

      catch {
        $::O(tree) insert $parent_id end -id $stack -text $line -values $v
      }
    }
  }

  set ::O(current) $zTest
  populate_index $db
}



set O(tree_sort) 0

::ttk::panedwindow .pan -orient horizontal
set O(tree) [scrollable ::ttk::treeview .pan.tree]

frame .pan.right
set O(text) [scrollable text .pan.right.text]
button .pan.right.index -command {populate_index mddb} -text "Show Index"
pack .pan.right.index -side top -fill x
pack .pan.right.text -fill both -expand true

$O(text) tag configure highlight -background wheat
$O(text) configure -wrap none -height 35

.pan add .pan.tree
.pan add .pan.right

$O(tree) configure     -columns {calls bytes}
$O(tree) heading #0    -text Line  -anchor w -command {sort_tree 0}
$O(tree) heading calls -text Calls -anchor w -command {sort_tree 2}
$O(tree) heading bytes -text Bytes -anchor w -command {sort_tree 4}
$O(tree) column #0    -width 150
$O(tree) column calls -width 100
$O(tree) column bytes -width 100

pack .pan -fill both -expand 1

#--------------------------------------------------------------------
# Open the database containing the malloc data. The user specifies the
# database to use by passing the file-name on the command line.
#

proc lsmtest_report_read {zReport} {
  sqlite3 mddb :memory:
  mddb eval {
    PRAGMA journal_mode=OFF;
    CREATE TABLE malloc(zTest, nCall, nByte, lStack);
    CREATE TABLE frame(frame PRIMARY KEY, function, line);
    CREATE TABLE file(name PRIMARY KEY, content);
  }

  set fd [open $zReport]
  set data [read $fd]
  close $fd

  set topic ""
  foreach zLine [split $data "\n"] {
    set list [split $zLine " "]
    if {[string is integer [lindex $list 0]]} {
      set nByte [lindex $list 0]
      set nCall [lindex $list 1]
      set lStack [lrange $list 2 end]
      mddb eval { INSERT INTO malloc VALUES($topic, $nCall, $nByte, $lStack) }
      foreach f $lStack { set aFrame($f) "" }
    } else {
      set topic [lindex $list 0]
    }
  }

  foreach k [array names aFrame] {
    set res [exec addr2line -f -e ./testfixture $k]

    set function [lindex $res 0] 
    set addr     [lindex $res 1]

    mddb eval { INSERT INTO frame VALUES($k, $function, $addr) }
    set aFile([lindex [split $addr :] 0]) ""
  }

  foreach f [array names aFile] {
    catch {
      set fd [open $f]
      set text [read $fd]
      close $fd
      mddb eval { INSERT INTO file VALUES($f, $text) }
    }
  }
}

proc open_database {} {
  set zFilename [lindex $::argv 0]
  if {$zFilename eq ""} {
    set zFilename malloc.txt
  }

  lsmtest_report_read $zFilename

  wm title . $zFilename

  mddb function lrange -argcount 3 lrange
  mddb function llength -argcount 1 llength
  mddb function trim_frames -argcount 1 trim_frames

  mddb eval {
    SELECT frame FROM frame 
    WHERE line LIKE '%mem.c:%' 
    OR function LIKE '%Malloc'
    OR function LIKE '%MallocRaw'
    OR function LIKE '%MallocZero'
    OR function LIKE '%Realloc'
  } {
    set ::O(ignore.$frame) 1
  }
}

open_database
bind $O(tree) <<TreeviewSelect>> [list populate_text_widget mddb]

populate_tree_widget mddb [mddb one {SELECT zTest FROM malloc LIMIT 1}]

