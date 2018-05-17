# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = CerebralCortexDataAnalysis
SOURCEDIR     = source
BUILDDIR      = build
SPHINXAPIOPTS = -fMeE
SPHINXAPIDOC  = sphinx-apidoc

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXAPIDOC) $(SPHINXAPIOPTS) -o "$(SOURCEDIR)" ../core/
	python3.6 doc_title_insert.py
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

