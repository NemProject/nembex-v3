#!/usr/bin/zsh

z=0; while :; do ps aux | grep '[p]ython3' && { (( z += 1 )); echo $z; if [ $z -gt 20 ]; then killall python3.4; fi; } || { z=0; }; sleep 3; done
