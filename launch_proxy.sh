#!/usr/bin/env sh

voms-proxy-destroy
voms-proxy-info -all
voms-proxy-init -valid 9999:00 -voms lofar:/lofar/user/sksp --pwstdin < $HOME/.grid
voms-proxy-info -all
