#
# Makefile to convert Quixote docs to HTML
#
# $Id$
#

TXT_FILES = $(wildcard *.txt)
HTML_FILES = $(TXT_FILES:%.txt=%.html)

RST2HTML = /www/python/bin/rst2html
RST2HTML_OPTS = -o us-ascii

%.html: %.txt
	$(RST2HTML) $(RST2HTML_OPTS) $< $@

all: $(HTML_FILES)

clean:
	rm -f $(HTML_FILES)
