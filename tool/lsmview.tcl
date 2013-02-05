#!/bin/sh
# \
exec wish "$0" "$@"

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

proc exec_lsmtest_show {args} {
  set fd [open [list |lsmtest show {*}$args 2>/dev/null]]
  set res ""
  while {![eof $fd]} { 
    set line [gets $fd]
    if {[regexp {^\#.*} $line]} continue
    if {[regexp {^Leaked*} $line]} continue
    append res $line
    append res "\n"
  }
  close $fd
  string trim $res
}

#############################################################################
# Constants used by the code that draws the graphical representation of
# the data structure (the boxes with the arrows and stuff). Changing these
# will change the space left between various elements on the canvas.
#
set G(segment_height)    20
set G(vertical_padding)  40
set G(svertical_padding)  5
#############################################################################


# Return the base-2 log of the value passed as the only argument.
#
proc log2 {i} {
  return [expr {log($i) / log(2)}]
}

# The following procs:
#
#   varset
#   link_varset
#   del_varset
#
# are used to partition up the contents of global array VARSET so that it
# can be shared between multiple widget objects.
#
proc varset {C nm args} {
  global VARSET
  if {[llength $args] > 1} { error "should be: varset WIDGET VAR ?VALUE?" }
  eval set [list VARSET($C.$nm)] $args
}
proc del_varset {C} {
  global VARSET
  foreach k [array names $C*] { unset VARSET($k) }
}
proc link_varset {C args} {
  foreach nm $args { uplevel upvar ::VARSET($C.$nm) $nm }
}

proc draw_segment {C segment tags} {
  foreach {iFirst iLast iRoot nSize} $segment {}

  set w [expr {$::G(scale) * [log2 [expr max($nSize, 2)]]}]
  set h $::G(segment_height)
  set y 0

  set w [expr max( $w,  [font measure default "1 pages"] )]

  # Draw the separators stack if required.
#   if {$iRoot} {
#     set septag "[lindex $tags end].sep"
#     set st [concat $tags $septag]
#     set w2 $w
#     for {set i 3} {$i > 0} {incr i -1} {
#       set w2 [expr $w/pow(2,$i)]
#       set h2 [expr $h/2]
# 
#       set x1 [expr ($w-$w2)/2]
#       set x2 [expr $x1+$w2]
# 
#       set id [$C create rect $x1 $y $x2 [expr $y+$h2]]
#       $C itemconfigure $id -outline black -fill white -tags $st
#       incr y $h2
#     }
# 
#     $C bind $septag <1> [list segment_callback $C $septag $segment]
#     $C bind $septag <Enter> [list segment_info $C $segment]
#     $C bind $septag <Leave> [list segment_info $C {}]
#   }

  set maintag "[lindex $tags end].main"
  set rt [concat $tags $maintag]
  $C create rectangle 0 $y $w [expr $y+$h] -outline black -fill white -tags $rt

  set tid [$C create text [expr $w/2] [expr $y+$h/2]]
  $C itemconfigure $tid -text "$nSize pages" -anchor center
  $C itemconfigure $tid -tags [concat $tags "[lindex $tags 0].text"]

  $C bind $maintag <1> [list segment_callback $C $maintag $segment]
  $C bind $maintag <Enter> [list segment_info $C $segment]
  $C bind $maintag <Leave> [list segment_info $C {}]
  $C bind $tid <1>     [list segment_callback $C $maintag $segment]
  $C bind $tid <Enter> [list segment_info $C $segment]
  $C bind $tid <Leave> [list segment_info $C {}]
}

proc segment_info {C segment} {
  set w $C
  while {[winfo class $w]!="Frame"} {
    set w [winfo parent $w]
    if {$w==""} return
  }
  set w $w.info
  if {$segment==""} {
    $w config -text ""
  } else {
    foreach {iFirst iLast iRoot nSize} $segment break
    $w config -text "first: $iFirst   last: $iLast\nroot: $iRoot   size: $nSize"
  }
}

proc segment_callback {C tag segment} {
  link_varset $C mySelected myScript

  if {[info exists mySelected]} { $C itemconfigure $mySelected -fill white }
  set mySelected $tag
  $C itemconfigure $mySelected -fill grey

  eval $myScript [list $segment]
}

proc draw_level {C level tags} {
  set lhs [lindex $level 1]

  set l [lindex $tags end]
  draw_segment $C $lhs [concat $tags "$l.s0"]
  foreach {x1 y1 x2 y2} [$C bbox "$l.s0"] {}

  set i 0
  set y 0
  set x [expr $x2+10]
  foreach seg [lrange $level 2 end] {
    set tag "$l.s[incr i]"
    draw_segment $C $seg [concat $tags $tag]

    $C move $tag $x $y
    foreach {x1 y1 x2 y2} [$C bbox $tag] {}
    set y [expr $y2+$::G(vertical_padding)]
  }
}

proc draw_structure {canvas structure} {
  link_varset $canvas mySelected myVertShift

  set C $canvas
  set W [winfo width $C]

  # Figure out the scale to use.
  # 
  set nMaxWidth 0.0
  foreach level $structure {
    set nRight 0
    foreach seg [lrange $level 2 end] {
      set sz [lindex $seg 3]
      if {$sz > $nRight} { set nRight $sz }
    }
    set nLeft [lindex $level 1 3]
    set nTotal [log2 [expr max($nLeft, 2)]]
    if {$nRight} {set nTotal [expr $nTotal + [log2 [expr max($nRight, 2)]]]}
    if {$nTotal > $nMaxWidth} { set nMaxWidth $nTotal }
  }

  if { $nMaxWidth==0.0 } { set nMaxWidth 1.0 }
  set ::G(scale) [expr (($W-100) / $nMaxWidth)]

  set y [expr $::G(vertical_padding) / 2]
  set l -1
  foreach level $structure {
    set tag "l[incr l]"

    draw_level $C $level $tag
    foreach {x1 y1 x2 y2} [$C bbox $tag] {}
    $C move $tag [expr ($W-$x2)/2] $y
    incr y [expr $y2 + $::G(vertical_padding)]

    set age [lindex $level 0]
    foreach {x1 y1 x2 y2} [$C bbox $tag.text] {}
    $C create text 5 $y1 -anchor nw -text [string toupper "${tag} ($age):"]
  }

  if {[info exists myVertShift]} {
    set H [winfo height $C]
    set region [$C bbox all]
    if {$y < $H} { $C move all 0 [expr $H-$y] }
  }

  set region [$C bbox all]
  lset region 0 0
  lset region 1 0
  $C configure -scrollregion $region

  if {[info exists mySelected]} { $C itemconfigure $mySelected -fill grey }
}

proc draw_one_pointer {C from to} {
  foreach {xf1 yf1 xf2 yf2} [$C bbox $from] {}
  foreach {xt1 yt1 xt2 yt2} [$C bbox $to] {}

  set line [$C create line [expr ($xf1+$xf2)/2] $yf2 [expr ($xt1+$xt2)/2] $yt1]
  $C itemconfigure $line -arrow last 
}

proc draw_internal_pointers {C iLevel level} {
  set iAge [lindex $level 0]      ;# Age of this level
  set lSeg [lrange $level 1 end]  ;# List of segments that make up this level

  for {set j 2} {$j < [llength $lSeg]} {incr j} {
    if {[lindex $lSeg $j 2]==0} {
      draw_one_pointer $C "l$iLevel.s[expr $j-1]" "l$iLevel.s$j"
    }
  }
}

proc draw_pointers {C structure} {
  for {set i 0} {$i < [llength $structure]} {incr i} {
    draw_internal_pointers $C $i [lindex $structure $i]
  }

  for {set i 0} {$i < ([llength $structure]-1)} {incr i} {
    set i2 [expr $i+1]

    set l1 [lindex $structure $i]
    set l2 [lindex $structure $i2]

    # Set to true if levels $i and $i2 are currently undergoing a merge
    # (have one or more rhs segments), respectively.
    #
    set bMerge1 [expr [llength $l1]>2]
    set bMerge2 [expr [llength $l2]>2]

    if {$bMerge2} {
      if {[lindex $l2 2 2]==0} {
        draw_one_pointer $C "l$i.s0" "l$i2.s1"
        if {$bMerge1} {
          draw_one_pointer $C "l$i.s[expr [llength $l1]-2]" "l$i2.s1"
        }
      }
    } else {

      set bBtree [expr [lindex $l2 1 2]!=0]

      if {$bBtree==0 || $bMerge1} {
        draw_one_pointer $C "l$i.s0" "l$i2.s0"
      }
      if {$bBtree==0} {
        draw_one_pointer $C "l$i.s[expr [llength $l1]-2]" "l$i2.s0"
      }
    }
  }
}

# Argument $canvas is a canvas widget. Argument $db is the Tcl data 
# structure returned by an LSM_INFO_DB_STRUCTURE call. This procedure
# clears the canvas widget, then draws a diagram representing the
# contents of $db. The canvas viewable region is set appropriately.
#
# The canvas widget is also configured with bindings so that the user
# may select any main or separators array using the pointer. Each time
# the selection changes, the page number of the first page in the selected
# array is appended to the list $script and the result evaulated.
#
proc draw_canvas_content {canvas db script} {
  link_varset $canvas myScript 
  set myScript $script
  draw_structure $canvas $db
  draw_pointers $canvas $db
}

# End of drawing code.
##########################################################################

proc dynamic_redraw {C S args} {
  $C delete all
  set db [lindex $::G(data) [$S get]]
  if {[llength $db]>0} {
    draw_canvas_content $C $db dynamic_callback
  }
}

proc dynamic_callback {args} {}

proc dynamic_input {C S args} {
  fileevent stdin readable {}
  if {[eof stdin]} return

  set line [gets stdin]
  if { [string range [string trim $line] 0 0]!="g" } {
    if {[llength $line] > 0 && $line != [lindex $::G(data) end]} { 
      lappend ::G(data) $line 
    }

    set end [expr [llength $::G(data)] - 1]
    if {$end>=0} { 
      $S configure -from 0 -to $end
      if {[$S get]==$end-1} { $S set $end }
    }
  }

  fileevent stdin readable [list dynamic_input $C $S]
}

proc dynamic_setup {} {
  global C S

  set C [scrollable canvas .c -background white -width 600 -height 600]
  set S [scale .s -orient horiz]
  pack .s -side bottom -fill x
  pack .c -expand 1 -fill both

  link_varset $C myVertShift
  set myVertShift 1

  bind $C <Configure>  [list dynamic_redraw $C $S]
  bind $C <KeyPress-q> exit
  focus $C
  $S configure -command [list dynamic_redraw $C $S]
  update
}

set ::G(data)    [list {}]


proc static_redraw {C} {
  link_varset $C myText myDb myData
  $C delete all
  draw_canvas_content $C $myData [list static_select_callback $C]
}

# The first parameter is the number of pages stored on each block of the
# database file (i.e. if the page size is 4K and the block size 1M, 256).
# The second parameter is a list of pages in a segment.
#
# The return value is a list of blocks occupied by the segment.
#
proc pagelist_to_blocklist {nBlkPg pglist} {
  set blklist [list]
  foreach pg $pglist {
    set blk [expr 1 + (($pg-1) / $nBlkPg)]
    if {[lindex $blklist end]!=$blk} {
      lappend blklist $blk
    }
  }
  set blklist
}

proc static_select_callback {C segment} {
  link_varset $C myText myDb myData myTree myCfg
  foreach {iFirst iLast iRoot nSize $segment} $segment {}

  set data [string trim [exec_lsmtest_show -c $myCfg $myDb array-pages $iFirst]]
  $myText delete 0.0 end

  # Delete the existing tree entries.
  $myTree delete [$myTree children {}]

  set nBlksz [expr [exec_lsmtest_show -c $myCfg $myDb blocksize] * 1024]
  set nPgsz  [exec_lsmtest_show -c $myCfg $myDb pagesize]

  if {[regexp {c=1} $myCfg]          || [regexp {co=1} $myCfg]
   || [regexp {com=1} $myCfg]        || [regexp {comp=1} $myCfg]
   || [regexp {compr=1} $myCfg]      || [regexp {compres=1} $myCfg]
   || [regexp {compress=1} $myCfg]   || [regexp {compressi=1} $myCfg]
   || [regexp {compressio=1} $myCfg] || [regexp {compression=1} $myCfg]
  } {
    set nBlkPg $nBlksz
  } else {
    set nBlkPg [expr ($nBlksz / $nPgsz)]
  }

  foreach pg $data {
    set blk [expr 1 + (($pg-1) / $nBlkPg)]
    if {[info exists block($blk)]} {
      incr block($blk) 1
    } else {
      set block($blk) 1
    }
  }
  foreach blk [pagelist_to_blocklist $nBlkPg $data] {
    set nPg $block($blk)
    set blkid($blk) [$myTree insert {} end -text "block $blk ($nPg pages)"]
  }
  foreach pg $data {
    set blk [expr 1 + (($pg-1) / $nBlkPg)]
    set id $blkid($blk)
    set item [$myTree insert $id end -text $pg]
    if {$pg == $iRoot} {
      set rootitem $item
      set rootparent $id
    }
  }

  if {[info exists rootitem]} {
    $myTree item $rootparent -open 1
    $myTree selection set $rootitem
  } else {
    $myTree item $id -open 1
    $myTree selection set [lindex [$myTree children $id] 0]
  }
}

proc static_page_callback {C pgno} {
  link_varset $C myText myDb myData myPgno myMode myCfg
  set myPgno $pgno
  set data [string trim [exec_lsmtest_show -c $myCfg $myDb $myMode $myPgno]]
  $myText delete 0.0 end
  $myText insert 0.0 $data
}

proc static_treeview_select {C} {
  link_varset $C myText myDb myData myTree
  set sel [lindex [$myTree selection] 0]
  if {$sel != ""} {
    set text [$myTree item $sel -text]
    if {[string is integer $text]} { static_page_callback $C $text }
  }
}

proc static_toggle_pagemode {C} {
  link_varset $C myMode myModeButton myPgno myText
  if {$myMode == "page-ascii"} {
    set myMode "page-hex"
    $myModeButton configure -text "Switch to ASCII"
  } else {
    set myMode "page-ascii"
    $myModeButton configure -text "Switch to HEX"
  }

  set y [lindex [$myText yview] 0]
  static_page_callback $C $myPgno
  $myText yview moveto $y
}

# Display the content of text widget $t in the editor identified by
# the $env(VISUAL) environment variable.
#
proc show_text_in_editor {t} {
  if {![info exists ::env(VISUAL)]} {
    tk_messageBox -type ok -title {No VISUAL} -message \
       "The VISUAL environment variable is not set"
    return
  }
  set fn [format temp-%08x%08x [expr {int(rand()*0xffffffff)}] \
                               [expr {int(rand()*0xffffffff)}]]
  set fd [open $fn w]
  puts $fd [$t get 1.0 end]
  flush $fd
  close $fd
  exec $::env(VISUAL) $fn &
  after 1000 [subst {catch {file delete -force $fn}}]
}

proc static_setup {zConfig zDb} {

  panedwindow .pan -orient horizontal
  frame .pan.c
  set C [scrollable canvas .pan.c.c -background white -width 400 -height 600]
  label .pan.c.info -border 2 -relief sunken -height 2
  pack .pan.c.info -side bottom -fill x 
  pack .pan.c.c -side top -fill both -expand 1

  link_varset $C myText myDb myData myTree mySelected myMode myModeButton myCfg
  set myDb $zDb

  frame .pan.t
  frame .pan.t.f
  set myModeButton [button .pan.t.f.pagemode]
  $myModeButton configure -command [list static_toggle_pagemode $C]
  $myModeButton configure -width 20
  $myModeButton configure -text "Switch to HEX"
  set myMode page-ascii
  set myCfg $zConfig

  set myText [scrollable text .pan.t.text -background white -width 80]
  button .pan.t.f.view -command [list show_text_in_editor $myText] \
                       -text Export
  pack .pan.t.f.pagemode .pan.t.f.view -side right
  pack .pan.t.f -fill x
  pack .pan.t.text -expand 1 -fill both

  set myTree [scrollable ::ttk::treeview .pan.tree]
  set myData [string trim [exec_lsmtest_show -c $myCfg $zDb]]

  $myText configure -wrap none
  bind $C <KeyPress-q> exit
  focus $C

  .pan add .pan.c .pan.tree .pan.t
  pack .pan -fill both -expand 1

  set mySelected "l0.s0.main"
  static_redraw $C
  bind $C <Configure>  [list static_redraw $C]
  bind $myTree <<TreeviewSelect>> [list static_treeview_select $C]

  static_select_callback $C [lindex $myData 0 1]
}

if {[llength $argv] > 2} {
  puts stderr "Usage: $argv0 ?CONFIG? ?DATABASE?"
  exit -1
}
if {[llength $argv]>0} {
  set zConfig ""
  set zDb [lindex $argv end]
  if {[llength $argv]>1} {set zConfig [lindex $argv 0]}
  static_setup $zConfig $zDb
} else {
  dynamic_setup
  fileevent stdin readable [list dynamic_input $C $S]
}
