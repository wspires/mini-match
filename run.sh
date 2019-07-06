#!/usr/bin/env bash

g++ -std=c++14 -O2 -Wall main.cpp -lpthread 2>&1 |tee run.log && ./a.out < cmd.txt 2>&1 |tee cmd.out
