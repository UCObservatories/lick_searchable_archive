from json import JSONEncoder
from astropy.io import fits


class FITSHeaderJSONEncoder(JSONEncoder):

    def default(self, o):
        if (isinstance(o, fits.PrimaryHDU) or
            isinstance(o, fits.ImageHDU) or 
            isinstance(o, fits.GroupsHDU) or
            isinstance(o, fits.BinTableHDU) or
            isinstance(o, fits.TableHDU)):
            
            header_dict = dict()
            for card in o.header.cards:
                header_dict[card.keyword] = card.value

            return header_dict
        return super().default(o)