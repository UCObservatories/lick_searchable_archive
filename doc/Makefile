# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXAPI     = sphinx-apidoc
SPHINXAPIOPT  = members,private-members,undoc-members,show-inheritance
SPHINXOPTS    = -aE -w ./sphinx_warnings.out
SPHINXBUILD   = sphinx-build
BUILDDIR      = _build
LOCALFILES    = $(BUILDDIR)/* api/*.rst sphinx_warnings.out
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(SPHINXOPTS) .

## Put it first so that "make" without argument is like "make help".
#help:
#	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
#
#.PHONY: help Makefile

clean:
	rm -rf $(LOCALFILES)

apirst:
	SPHINX_APIDOC_OPTIONS=$(SPHINXAPIOPT) $(SPHINXAPI) --separate -o ./api ../lick_searchable_archive  ../lick_searchable_archive/version.py

html:
	make apirst
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

htmlonly:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

## Catch-all target: route all unknown targets to Sphinx using the new
## "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
#%: Makefile
#	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
