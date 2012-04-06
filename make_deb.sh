#!/bin/sh
#
# This script makes a simple deb package of the pig code

DEB_NAME='rgpig'
CODE_PATH='opt/realgo/rgpig'

date=`date +%Y-%m-%d-%H%M`
umask 0022

#Prep the dir
mkdir /tmp/$DEB_NAME
mkdir -p /tmp/$DEB_NAME/$CODE_PATH
cp -r `dirname $0`/* /tmp/$DEB_NAME/$CODE_PATH
mkdir /tmp/$DEB_NAME/DEBIAN
cat >> /tmp/$DEB_NAME/DEBIAN/control <<EOF
Package: $DEB_NAME
Version: $date
Section: web
Priority: optional
Architecture: all
Maintainer: RealGo <sysadm@realgo.com>
Description: Pig scripts for the processing of web access logs
EOF

#Make the package
fakeroot dpkg-deb -b /tmp/$DEB_NAME ./$DEB_NAME.deb

#Clean up
rm -r /tmp/$DEB_NAME
