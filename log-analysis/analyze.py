import sys
import os
import subprocess
import os.path
import re

def getOut(cmd, file=None):
   if file == None:
       file = mainfile
   return subprocess.check_output(cmd % file, shell = True).rstrip('0').replace('V1', '')

def getSummary(cmd, file=None):
    return getOut("cat %%s | %s | ~/bin/r summary -" % cmd, file)

meanre = re.compile('\s*Mean\s*:\s*([\d.]+)', re.MULTILINE)
def getAvg(summary):
    m = meanre.search(summary)
    #print m, summary
    return float(m.group(1))

logfile = sys.argv[1]
outfile = open(logfile + ".out.md", "w")
mainfile = logfile + ".part"

if not os.path.exists(mainfile):
    print "Splitting %s into parts" % logfile
    os.system("perl split.pl %s" % logfile)

print ("Main file %s " % mainfile)

batches = int(getOut("cat %s | grep 'changes in batch' | wc -l"))
print "%d batches in file" % batches
outfile.write("Logfile %s contains %d batches\n\n" % (logfile, batches))
outfile.write("= Batch Stats =\n")

modified_triples = getSummary("grep 'and modified' | cut -d ' ' -f 13")
modified_avg = getAvg(modified_triples)

changes_per_batch = getSummary("grep 'changes in batch' | cut -d ' ' -f 6")
print "%f changes per batch" % getAvg(changes_per_batch)
outfile.write("Changes per batch:\n%s\n" % changes_per_batch)

filtered_per_batch = getSummary("grep 'Filtered' | cut -d ' ' -f 9")
print "%f real changes per batch" % getAvg(filtered_per_batch)
outfile.write("Filtered changes per batch:\n%s\n" % filtered_per_batch)
filter_avg = getAvg(filtered_per_batch)

time_per_batch =  getSummary("grep 'Polled up to' | cut -d ' ' -f 1 | perl -ne '/(\d+):(\d+):(\d+)\.(\d+)/; $ms = $4+1000*($3+60*($2+60*$1)); if($lastms) { print $ms-$lastms, chr(0xA); } $lastms = $ms;'")
outfile.write("Time per batch (ms):\n%s\n" % time_per_batch)
time_avg = getAvg(time_per_batch)

advance_per_batch = getSummary("grep 'Polled up to' | cut -d ' ' -f 10 | perl -ne '/T(\d+):(\d+):(\d+)Z/; $ms = ($3+60*($2+60*$1)); if($lastms) { print $ms-$lastms,chr(0xA); } $lastms = $ms;'")
outfile.write("Advance per batch (s):\n%s\n" % advance_per_batch)
adv_avg = getAvg(advance_per_batch)

outfile.write("Modified triples:\n%s\n" % modified_triples)

outfile.write("Modified triples - %f per entity\n" % (modified_avg/filter_avg))

outfile.write("= Processing Stats =\n")

outfile.write("Processing time per change: %f ms, %f updates/second\n" %(time_avg/filter_avg, filter_avg/time_avg*1000))
print ("Processing time per change: %f ms, %f updates/second\n" %(time_avg/filter_avg, filter_avg/time_avg*1000))

print "Stream is %f updates/s\n" % (filter_avg/adv_avg)
outfile.write("\nStream is %f updates/s\n" % (filter_avg/adv_avg))
outfile.write("\nUpdate/stream ratio is %f\n\n" % (adv_avg/time_avg*1000))

update_preparation = getSummary("grep 'Preparing update' | cut -d ' ' -f 10")
outfile.write("Update preparation time (ms):\n%s\n" % update_preparation)

sparqlname = logfile + ".q"
if not os.path.exists(sparqlname):
    os.system("cat %s | perl getsparql.pl | sort > %s" %(logfile, sparqlname))
print "Queries in %s" % sparqlname

filter_times = getSummary("grep 'SELECT DISTINCT ?s WHERE' | cut -d ' ' -f 7", sparqlname)
update_times = getSummary("grep 'Clear' | cut -d ' ' -f 9", sparqlname)
tsupdate_times = getSummary("grep 'DELETE {' | cut -d ' ' -f 4", sparqlname)
values_times = getSummary("grep 'SELECT DISTINCT ?entity ?s' | cut -d ' ' -f 6", sparqlname)

outfile.write("== Query Stats ==\n")
outfile.write("FILTER query (ms):\n%s\n" % filter_times)
outfile.write("SELECT values query (ms):\n%s\n" % values_times)
outfile.write("UPDATE query (ms):\n%s\n" % update_times)
outfile.write("Timestamp UPDATE query (ms):\n%s\n" % tsupdate_times)

update_avg = getAvg(update_times)

outfile.write("Update query speed is %f entities/s, %f triples/s\n" % (filter_avg/update_avg*1000, modified_avg/update_avg*1000))
os.system("cat %s.out.md | pbcopy" % logfile)
