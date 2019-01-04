#!/bin/bash
python raftnode.py config.txt 0 6001&
python raftnode.py config.txt 1 6002&
python raftnode.py config.txt 2 6003&