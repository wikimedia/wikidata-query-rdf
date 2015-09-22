
$f = $ARGV[0];

open $in, $ARGV[0] or die $!;
%FILES = {};
$out = '';
$outf = STDOUT;

while(<$in>) {
	if(/\[(main|update (\d+))\]/) {
		$newout = $1;
		if($out ne $newout) {
			$outf = switchout($newout, $2);
			$out = $newout;
		}
	}
	print $outf $_;
}

sub switchout {
	my ($newout, $name) = @_;
	if(!defined($FILES{$newout})) {
		my $f = $ARGV[0].".part".$name;
		print "Opening $f for $newout\n";
		open $FILES{$newout}, ">", $f;
	}
	return $FILES{$newout};
}
