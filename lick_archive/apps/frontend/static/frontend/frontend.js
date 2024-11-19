const objectRadioInput = document.getElementById("query_by_object")
const dateRadioInput = document.getElementById("query_by_date")
const filenameRadioInput = document.getElementById("query_by_file")
const locationRadioInput = document.getElementById("query_by_coords")
const countRadioInput = document.getElementById("id_count_0")
const resultsRadioInput = document.getElementById("id_count_1")

const objectFields = document.getElementById("id_object_fields")
const dateFields = document.getElementById("id_date_fields")
const filenameFields = document.getElementById("id_filename_fields")
const locationFields = document.getElementById("id_coords_fields")
const resultFields = document.getElementById("fields")

const instrAllButton = document.getElementById("instrument_all")
const instrNoneButton = document.getElementById("instrument_none")
const instrumentCheckboxes = document.getElementsByClassName("search_instr_check")

const resultHdrCheckbox = document.getElementById("search_results_hdr_select")
const downloadSelectedButtons = document.querySelectorAll("button[id^='download_selected'")
const downloadAllButtons = document.querySelectorAll("button[id^='download_all'")
const resultsTable = document.getElementById("search_results_table")

objectRadioInput.addEventListener("change", enableAndDisableFields)
dateRadioInput.addEventListener("change", enableAndDisableFields)
filenameRadioInput.addEventListener("change", enableAndDisableFields)
locationRadioInput.addEventListener("change", enableAndDisableFields)
countRadioInput.addEventListener("change", enableAndDisableResultFields)
resultsRadioInput.addEventListener("change", enableAndDisableResultFields)

instrAllButton.addEventListener("click", selectInstruments)
instrNoneButton.addEventListener("click", selectInstruments)

resultHdrCheckbox.addEventListener("click", selectResults)

const resultCheckboxes = document.querySelectorAll("input[id^='result_select_'")
for (i=0; i<resultCheckboxes.length; i++) {
    resultCheckboxes[i].addEventListener("click", rowSelected)
}
let numRowsSelected = 0

for (i=0; i<downloadSelectedButtons.length; i++) {
    downloadSelectedButtons[i].addEventListener("click",downloadSelected)
    downloadSelectedButtons[i].disabled = true
}
for (i=0; i<downloadAllButtons.length; i++) {
    downloadAllButtons[i].addEventListener("click",downloadAll)
}

document.addEventListener("DOMContentLoaded", enableAndDisableFields)

function selectResults(event) {
    if (event.target.indeterminate == true) {
        value = false
        numRowsSelected = 0
    } else if (event.target.checked == true) {
        value = true
        numRowsSelected = resultCheckboxes.length
    }
    else {
        value = false
        numRowsSelected = 0
    }
    for (i=0; i<resultCheckboxes.length; i++) {
        resultCheckboxes[i].checked=value
    }
    for (i=0; i<downloadSelectedButtons.length; i++) {
        downloadSelectedButtons[i].disabled = (numRowsSelected == 0)
    }

}

function rowSelected(event) {
    if (event.target.checked == true) {
        numRowsSelected++
    }
    else {
        numRowsSelected--
    }
    if (numRowsSelected < 0) {
        numRowsSelected = 0
    } else if (numRowsSelected > resultCheckboxes.length) {
        numRowsSelected = resultCheckboxes.length
    }
    for (i=0; i<downloadSelectedButtons.length; i++) {
        downloadSelectedButtons[i].disabled = (numRowsSelected == 0)
    }
    if (numRowsSelected == 0) {
        resultHdrCheckbox.checked = false
        resultHdrCheckbox.indeterminate = false
    } else if (numRowsSelected == resultCheckboxes.length) {
        resultHdrCheckbox.checked = true
        resultHdrCheckbox.indeterminate = false
    }
    else {
        resultHdrCheckbox.checked = false
        resultHdrCheckbox.indeterminate = true
    }
}

function downloadSelected(event) {
    for (const checkbox of resultCheckboxes) {
        if (checkbox.checked == true) {
            resultIdx = checkbox.id
        }
    }
}
function selectInstruments(event) {
    if (event.target.id=="instrument_all") {
        value = true
    } else if (event.target.id == "instrument_none") {
        value = false
    }
    else {
        return
    }
    for (checkbox of instrumentCheckboxes) {
        checkbox.checked = value
    }
}

function enableAndDisableResultFields() {
    if (countRadioInput.checked) {
        resultFields.disabled=true
    }
    else {
        resultFields.disabled=false
    }
}

function enableAndDisableFields() {

    if (objectRadioInput.checked) {
        objectFields.disabled=false
        dateFields.disabled=true
        filenameFields.disabled=true
        locationFields.disabled=true
    } else if (dateRadioInput.checked) {
        objectFields.disabled=true
        dateFields.disabled=false
        filenameFields.disabled=true
        locationFields.disabled=true
    }
    else if (filenameRadioInput.checked) {
        objectFields.disabled=true
        dateFields.disabled=true
        filenameFields.disabled=false
        locationFields.disabled=true
    } else {
        objectFields.disabled=true
        dateFields.disabled=true
        filenameFields.disabled=true
        locationFields.disabled=false
    }
}
