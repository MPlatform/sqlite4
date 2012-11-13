
if {[llength $argv]!=1} { 
  puts stderr "Usage: $argv0 FILENAME"
  exit -1
}
set zFile [lindex $argv 0]
if {![file exists $zFile]} {
  puts stderr "No such file: $zFile"
  exit -1
}
set fd [open $zFile]
set out ""

set idx(1) 0
set idx(2) 0
set idx(3) 0

set iSeenToc 0
set before_toc ""
set after_toc ""
set out ""

while {![eof $fd]} {
  set line [gets $fd]

  if {[string trim $line]=="<div id=start_of_toc></div>"} {
    if {$iSeenToc!=0} {error "start_of_toc mismatch"}
    set iSeenToc 1
    set before_toc $out
    set out ""
    continue
  }
  if {[string trim $line]=="<div id=end_of_toc></div>"} {
    if {$iSeenToc!=1} {error "end_of_toc mismatch"}
    set iSeenToc 2
    continue
  }
  if {$iSeenToc==1} continue

  if {[regexp {<h([123]).*>} $line -> iLevel]} {
    if {$iLevel==1 || $idx(1)>0} {
      incr idx($iLevel)
      for {set i [expr $iLevel+1]} {$i < 4} {incr i} { set idx($i) 0 }
      set num ""
      for {set i 1} {$i <= $iLevel} {incr i} { append num "$idx($i)." }

      regsub -all {<[^>]*>} $line "" tag
      regsub -all {[0123456789.]} $tag "" tag
      set tag [string map {" " _} [string trim [string tolower $tag]]]

      set pattern [subst -nocom {<h$iLevel[^>]*>[0123456789. ]*}]
      regsub $pattern $line "<h$iLevel id=$tag>$num " line

      regsub -all {<[^>]*>} $line "" text
      set margin [string repeat {&nbsp;} [expr $iLevel * 6]]
      append toc "${margin}<a href=#$tag style=text-decoration:none>$text</a><br>\n"
    }
  }
  append out "$line\n"
}
if {$iSeenToc!=2} {error "No toc divs..."}
set after_toc $out



puts $before_toc
puts "<div id=start_of_toc></div>"
puts $toc
puts "<div id=end_of_toc></div>"
puts $after_toc




