#!/usr/bin/env bash

g++ -std=c++14 -O2 -Wall main.cpp -o mini-match -lpthread 2>&1 |tee run.log && ./mini-match < cmd.txt 2>&1 |tee cmd.out
