#!/bin/bash
# This script can be used in X-Road v6 security servers to streamline analyzing archived messagelog files.
# Slower version, but additionally outputs SigningTime

TMP_DIR=`mktemp -d`

function cleanup {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT # Execute "cleanup" function on exit

if [ -z $1 ] || [ $1 = "-h" ] || [ $1 = "--help" ]; then
    echo -e "Usage: cat_mlog.sh files"
    exit 1;
fi

for zfile in "$@"; do
    if [ -f $zfile ]; then
        unzip -q $zfile "*.asice" -d $TMP_DIR && (
            cd $TMP_DIR
            for afile in *.asice; do
                echo "${zfile} -> ${afile}:"
                unzip -q -p $afile META-INF/signatures.xml | perl -e 'my $stdin = join("", <STDIN>); print "Signing time: $1\n" if $stdin =~ /<xades:SigningTime>(.*)<\/xades:SigningTime>/s'
                unzip -q -p $afile message.xml
                echo # Newline
            done
            rm -f $TMP_DIR/*
        )
    else
        echo "File \"$zfile\" not found"
    fi
done
