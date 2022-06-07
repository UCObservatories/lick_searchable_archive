from readers.metadata_reader import MetadataReader
from astropy.io import fits

# Add each new MetadataReader subclass here
import readers.shane_kast
import readers.shane_ao_sharcs



def read_row(file_path):
    is_fits = False
    try:
        with fits.open(file_path) as hdul:
            is_fits = True
            for child in MetadataReader.__subclasses__():
                if child.can_read(file_path, hdul):
                    return child().read_row(file_path, hdul)
            raise ValueError(f"Unknown FITS file: {file_path}")
    except Exception as e:
        if is_fits:
            # Propagate exception to caller
            raise
        else:
            # The file isn't a fits file. This may not be an error if it's a supported jpg or mpg type
            # but until those types are supported just raise an exception
            raise ValueError(f"Unrecognized file type: {file_path}.")
