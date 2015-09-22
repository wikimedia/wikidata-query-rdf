while(<>) {
if(/(\d+):(\d+):(\d+)\.(\d+)/) {
	 $ms = $4+1000*($3+60*($2+60*$1)); 
	if($lastms) { print $ms-$lastms, "\n"; } 
	$lastms = $ms;
}
}
