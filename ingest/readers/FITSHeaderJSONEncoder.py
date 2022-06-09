from json import JSONEncoder
from astropy.io import fits


class FITSHeaderJSONEncoder(JSONEncoder):

    def default(self, o):
        if (isinstance(o, fits.PrimaryHDU) or
            isinstance(o, fits.ImageHDU) or 
            isinstance(o, fits.GroupsHDU) or
            isinstance(o, fits.BinTableHDU) or
            isinstance(o, fits.TableHDU)):
            return list(o.header.cards)
        elif isinstance(o, fits.Card):
            return {"keyword": o.keyword,
                    "value": o.value,
                    "comment": o.comment }
        return super().default(o)