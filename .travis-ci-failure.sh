#!/bin/bash

cd /tmp
git config --global user.email "travis@travis-ci.org"
git config --global user.name "Travis"

#using token clone gh-pages branch
git clone --quiet https://${GH_TOKEN}@github.com/jonludlam/vhdd-logs.git > /dev/null

cd vhdd-logs
mkdir $TRAVIS_BUILD_NUMBER
cp /tmp/vhdd.* $TRAVIS_BUILD_NUMBER
git add $TRAVIS_BUILD_NUMBER
git commit -m "Travis build $TRAVIS_BUILD_NUMBER logs"
git push -fq origin master > /dev/null

