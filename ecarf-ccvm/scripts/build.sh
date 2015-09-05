#!/bin/bash
cd ecarf
git pull
mvn clean compile install -Dmaven.test.skip=true
cd

