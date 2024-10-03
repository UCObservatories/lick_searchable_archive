TEST_USER = "test_user"
TEST_USER_OWNERHINT = "test user.testing user"
PUBLIC_FILE = '2019-05/23/shane/r33.fits'
PRIVATE_FILE = "ext_test_file_r33.fits"
TEST_INSTR = "shane"

def enable_user(user_name):
    from lick_archive.apps.archive_auth.models import ArchiveUser
    test_user = ArchiveUser.objects.filter(username=user_name)[0]
    test_user.is_active = True
    test_user.save()

def disable_user(user_name):
    from lick_archive.apps.archive_auth.models import ArchiveUser
    test_user = ArchiveUser.objects.filter(username=user_name)[0]
    test_user.is_active = False
    test_user.save()

def add_override_access(override_date, override_instr, user_ownerhint, filename):
    from lick_archive.apps.archive_auth.models import DBOverrideAccessFile

    db_oafs = list(DBOverrideAccessFile.objects.filter(night=override_date,instrument_dir=override_instr).order_by("-sequence_id"))
    if len(db_oafs) > 0:
        oaf = db_oafs[0]
    else:
        # Make oaf
        oaf = DBOverrideAccessFile(night=override_date,instrument_dir=override_instr,sequence_id=0)
        oaf.save()

    oaf_rules = list(oaf.rules.filter(pattern=filename, access='ownerhints'))
    if len(oaf_rules) == 0:
        oaf_rule = oaf.rules.create(pattern=filename, access="ownerhints")
    else:
        oaf_rule = oaf_rules[0]
        
    db_ownerhints = list(oaf_rule.ownerhints.filter(ownerhint=user_ownerhint))
    if len(db_ownerhints) == 0:
        oaf_rule.ownerhints.create(ownerhint=user_ownerhint)

def remove_override_access(override_date, override_instr, user_ownerhint, filename):
    from lick_archive.apps.archive_auth.models import DBOverrideAccessFile, DBOverrideAccessRule, DBOwnerhint

    db_oafs = list(DBOverrideAccessFile.objects.filter(night=override_date,instrument_dir=override_instr).order_by("-sequence_id"))
    if len(db_oafs) > 0:
        oaf = db_oafs[0]

        oaf_rules = list(oaf.rules.filter(pattern=filename, access='ownerhints'))
        if len(oaf_rules) > 0:
            for oaf_rule in oaf_rules:           
                oaf_rule.ownerhints.filter(ownerhint=user_ownerhint).delete()
                if oaf_rule.ownerhints.count() == 0:
                    oaf_rule.delete()
        if oaf.rules.count() == 0:
            oaf.delete()
 
def replace_parsed_url_hostname(parsed_url, hostname):
    if parsed_url.port is not None:
        port = f":{parsed_url.port}"
    else:
        port = ""

    return parsed_url._replace(netloc=f"{hostname}{port}").geturl()

