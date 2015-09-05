#!/bin/bash
cd ecarf/ecarf-ccvm/
mvn -q exec:java -Dexec.args="/home/omerio/job.json" > /home/omerio/output.log 2>&1 &
cd
tail -f output.log

