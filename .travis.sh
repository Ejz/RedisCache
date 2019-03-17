#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

test -f composer.phar || curl -sS 'https://getcomposer.org/installer' | php
test -f phpunit.phar || wget "https://phar.phpunit.de/phpunit-6.4.phar" -O phpunit.phar
test -n "$GH_TOKEN" && php composer.phar config -g github-oauth.github.com "$GH_TOKEN"
chmod a+x *.phar

./composer.phar install
./phpunit.phar
