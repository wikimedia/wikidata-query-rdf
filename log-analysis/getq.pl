$qtype = undef;
while(<>) {
	if(/Running SPARQL/) {
		$qtype = 'other';
		$cnt = 1;
	}
	if(/Completed in (\d+)/) {
		if($qtype) {
			print "$1 $qtype $cnt\n";
		}
		$qtype = undef;
		next;
	}	
	next unless $qtype;
	$cnt++;
	if(/ASK {/) {
		$qtype = 'version';
	}
	if(/SELECT DISTINCT \?s/) {
		$qtype = 'fetch'
	}
	if($qtype eq 'fetch' && m|<http://www.w3.org/ns/prov#wasDerivedFrom>|) {
		$qtype = 'refs';
	}
	if($qtype eq 'refs' && /} UNION {/) {
		$qtype = 'values';
	}
	if(/INSERT {/) {
		$qtype = "update";
	}
}
	
