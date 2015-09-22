$sparql = 0;
$name = "wtf";
while(<>) {
	if(!$sparql) {
		if(/Running SPARQL: (.+)/) {
			$name = $1;
			$sparql = 1;
		}
	} else {
		if(/- Completed in (\d+) ms/) {
			$sparql = 0;
			print "$name => $1\n";
		}
	}
}