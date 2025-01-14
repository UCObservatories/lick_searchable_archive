ROOTPATH := ../..
DEST_DIR := ../build/html

all: fields_html

fields_rst:
	mkdir -p $(DEST_DIR)
	export PYTHONPATH=$(ROOTPATH);python $(ROOTPATH)/scripts/build_data_dictionary_docs.py $(DEST_DIR)/fields.rst

fields_html: fields_rst ../templates/archive_base_template.jinja
	rst2html5 --template ./template.txt $(DEST_DIR)/fields.rst $(DEST_DIR)/fields.jinja
	python3 ../build_scripts/render_jinja_to_html.py $(DEST_DIR)/fields.jinja $(DEST_DIR)/fields.html -v pagename=fields -p ..

clean: 
	rm -r $(DEST_DIR)
