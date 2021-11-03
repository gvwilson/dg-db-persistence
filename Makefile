JEKYLL=jekyll
SITE=./_site

CONFIG=_config.yml
INCLUDES=$(wildcard _includes/*.html)
LAYOUTS=$(wildcard _layouts/*.html)
STATIC=$(wildcard static/*.*)
SOURCE=index.html

.DEFAULT: commands

## commands: show available commands
commands:
	@grep -h -E '^##' ${MAKEFILE_LIST} | sed -e 's/## //g' | column -t -s ':'

## build: rebuild site without running server
build: ${SUPPORT}
	${JEKYLL} build

## serve: build site and run server
serve: ${SUPPORT}
	${JEKYLL} serve

## clean: clean up stray files
clean:
	@find . -name '*~' -exec rm {} \;
	@rm -rf ${SITE}
