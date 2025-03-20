import {LoginControls} from "./login_controls.js"
import {LickArchiveClient} from "./lick_archive_client.js"
import {ErrorSection} from "./error_section.js"
import "./theme.js"

const errorSection = new ErrorSection()
const archiveClient = new LickArchiveClient()
const loginControls = new LoginControls(archiveClient, errorSection)
