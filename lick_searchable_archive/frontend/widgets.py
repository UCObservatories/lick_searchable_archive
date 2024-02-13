import logging

logger = logging.getLogger(__name__)

from django import forms

class OperatorWidget(forms.MultiWidget):

    def __init__(self, subwidgets, names, class_prefix, attrs=None, labels=[], modifier=None):

        #widgets={"operator": forms.Select(choices=operators, attrs={"class": class_prefix+"operator"})}
        self.class_prefix = class_prefix
        self.labels=labels

        if len(names) != len(subwidgets):
            raise ValueError("Length of names must match length of subwidgets")        
        widgets = dict()
        for i, subwidget in enumerate(subwidgets):
            if len(names[i]) == 0:
                subwidget.attrs["class"] = class_prefix + "value"
            else:
                subwidget.attrs["class"] = class_prefix + names[i]
            widgets[names[i]] = subwidget

        super().__init__(widgets,attrs)
        logger.debug(f"my name: all subwidgets: {subwidgets} widgets: {widgets}")
        self.template_name = "widgets/operator_widget.html"

    def decompress(self, value):
        return_value = []
        if isinstance(value,dict):
            logger.debug("Decompressing dict")
            if "modifier" in value:
                return_value = [value["operator"], value["modifier"], value["value"]]
            else:
                return_value = [value["operator"], value["value"]]
        else:
            return_value = ['','','']
        logger.debug(f"Returning: {return_value}")
        return return_value

    def get_context(self, name, value, attrs):
        context = super().get_context(name, value, attrs)
        for i, subwidget in enumerate(context["widget"]["subwidgets"]):
            if i < len(self.labels) and self.labels[i] is not None:
                subwidget["wrap_label"] = True                
                subwidget["label"] = self.labels[i]
            logger.debug(f"subwidget context {subwidget}")
        return context

class PageNavigationWidget(forms.Widget):

    def __init__(self, attrs, form_id, max_controls, num_surrounding_controls=2):
        super().__init__(attrs)
        self.template_name = "widgets/page_navigation.html"
        self.max_controls = max_controls
        self.num_surrounding_controls=num_surrounding_controls
        self.total_pages=1
        self.form_id = form_id

    def determine_pages(self, current_page):
        need_start_ellipses = False
        need_end_ellipses = False

        if not isinstance(current_page, int):
            current_page = int(current_page)

        # The page list always starts with the previous page and the first page. A negative previous page is allowed, and
        # renders to a disabled HTML control.
        page_list=[(current_page - 1, "<"), (1, "1")]

        if self.total_pages <= self.max_controls -2 :
            # There are enough controls to show all the pages
            page_list += list(zip(range(2,self.total_pages),[str(n) for n in range(2,self.total_pages)]))
        else:
            # All of the page numbers won't fit within the desired number of controls, some ellipses will be needed.

            # At low pages numbers the ellipses will be before the last page. 
            # For example with max_controls = 12, total_pages=100, current_page = 2, surrounding = 2 
            #     <<  1 <2> 3  4  5  6  7  8  ...100 >>
            # This will continue to be the case until there aren't enough controls to hold the surrounding pages. This is the change over
            # point. In this example the change over is between pages 6 and 7
            #     <<  1  2  3  4  5 <6> 7  8  ...100 >>
            #     <<  1 ... 4  5  6 <7> 8  9  ...100 >>
            
            # To find the change over point, find the last page that could be displayed without elipses after the 1 (max_controls -4)
            # and subtract the number of surrounding pages. The 4 includes the previous page and next page controls, the last page control,
            # and the ellipses before the last page.
            change_over = self.max_controls-(4+self.num_surrounding_controls)

            if current_page <= change_over:
                # The current page is before the change over, only one ellipses is needed, at the end
                need_end_ellipses = True
                
                # The range of numbers up to but not included the ellipses before the last page
                start_range=2
                end_range=self.max_controls-4
            else:
                # After the change over, there will be one ellipses at the start, and potentially one at the end
                need_start_ellipses = True

                if current_page+self.num_surrounding_controls < self.total_pages-2:
                    # The current page is far enough away from the last page to require ellipses at the end
                    need_end_ellipses = True

                    # End the range with the last of the surrounding pages around the current_page
                    end_range=current_page+self.num_surrounding_controls

                    # Start the range with enough pages to fill up the maximum number of controls 
                    # (-6 to exclude for the two ellipses, previous and next page, and first and last page)
                    start_range = (end_range-(self.max_controls-6))+1
                else:
                    # The current page is close enough to the final page that no ellipses are needed at the end
                    # End the range just before the final page
                    end_range=self.total_pages-1
                    # Start the range with enough pages to fill up the maximum allowed controls (-5 for one ellipses, the previous and next page,
                    # and the first and last page)
                    start_range=(end_range-(self.max_controls-5))+1

            # Build the pages between the first and last page, adding ellipses if needed
            if need_start_ellipses:
                page_list.append(("...","..."))

            page_list += list(zip(range(start_range,end_range+1),[str(n) for n in range(start_range,end_range+1)]))

            if need_end_ellipses:
                page_list.append(("...","..."))

        # The page list ends with the last page and the previous page
        if self.total_pages > 1:
            page_list.append((self.total_pages, str(self.total_pages)))

        page_list.append((current_page+1, ">"))

        return page_list

    def format_value(self, value):
        return int(value)

    def get_context(self, name, value, attrs):
        default_context = super().get_context(name, value, attrs)
        # Set the total pages from our choices value
        default_context['widget']['total_pages'] = self.total_pages
        default_context['widget']['page_list'] = self.determine_pages(value)
        default_context['widget']['form_id'] = self.form_id

        logger.debug(f"Returning: {default_context['widget']}")
        return default_context

def test_determine_pages():
    w = PageNavigationWidget({}, "form", 12, 2)
    for total_pages in range(1,16):
        w.total_pages = total_pages
        for page in range(1, total_pages+1):
            l = w.determine_pages(page)
            print(f"{page:-2} of {total_pages:-2} length({len(l):-2}) {l}")

