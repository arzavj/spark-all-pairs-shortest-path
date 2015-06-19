
import subprocess
import re
import random

def main():
    base_cmd = "sbt \"run-main AllPairsShortestPath %s %s %s %s\""
    stepRange = [50]
    intervals = [2]
    n = 500

    for k in range(1, 2):
        random.shuffle(intervals)
        for i in intervals:
            m = n/2
            random.shuffle(stepRange)	
            for stepSize in stepRange:
                cmd = base_cmd % (n, m, stepSize, i)
                output = subprocess.check_output(cmd, shell=True)
                for line in output.splitlines():
                    matches = re.match(r'Elapsed time: ([0-9]+)s', line)
                    if matches:
                        print "%s,%s,%s,%s,%s" % (n, m, stepSize, i, matches.group(1))
                        break

if __name__ == "__main__":
    main()
