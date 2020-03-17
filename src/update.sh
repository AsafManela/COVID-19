#!/bin/bash

git checkout master
git fetch --all
git rebase upstream/master 
git checkout amplots
git rebase master