from astropy.io import fits


class MockHDU:
    """Mock HDU object for unit testing. 
       If any of our code starts touching data, a real HDU object may be needed
    """
    def __init__(self, header):
        self.header = header

def get_hdul_from_text(text_files):
    """
    Build a mock HDU list from headers written to text files
    """
    hdul = [] 
    for file in text_files:
        hdul.append(MockHDU(fits.Header.fromfile(file, sep='\n', endcard=False, padding=False)))

    return hdul

