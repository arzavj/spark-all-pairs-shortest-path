
import subprocess
import re

def main():
    base_cmd = "sbt \"run-main AllPairsShortestPath %s %s %s\""
    for n in range(100, 200, 50):
        m = n/2
        for stepSize in range(20, m + 1, 20):
            cmd = base_cmd % (n, m, stepSize)
            output = subprocess.check_output(cmd, shell=True)
            for line in output.splitlines():
                matches = re.match(r'Elapsed time: ([0-9]+)s', line)
                if matches:
                    print "%s,%s,%s,%s" % (n, m, stepSize, matches.group(1))
                    break

if __name__ == "__main__":
    main()