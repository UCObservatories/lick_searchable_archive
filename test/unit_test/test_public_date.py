# Test the date arithmetic used to calculate the date a file 
# leaves it's proprietary period
import pytest

from datetime import date
from lick_archive.metadata.metadata_utils import calculate_public_date

def test_invalid_period():

    file_date = date(2023, 1, 10)

    with pytest.raises(ValueError,match="Invalid proprietary period"):
        public_date = calculate_public_date(file_date, "year")

    with pytest.raises(ValueError,match="Invalid proprietary period"):
        public_date = calculate_public_date(file_date, "")

    with pytest.raises(ValueError,match="Invalid proprietary period"):
        public_date = calculate_public_date(file_date, "3 to 4 years")

    with pytest.raises(ValueError,match="Proprietary period does not contain a valid positive integer"):
        public_date = calculate_public_date(file_date, "a year")

    with pytest.raises(ValueError,match="Proprietary period does not contain a valid positive integer"):
        public_date = calculate_public_date(file_date, "1.1 years")

    with pytest.raises(ValueError,match="Proprietary period must be a positive integer"):
        public_date = calculate_public_date(file_date, "-1 years")

    with pytest.raises(ValueError,match="Proprietary period must be a positive integer"):
        public_date = calculate_public_date(file_date, "0 years")

    with pytest.raises(ValueError,match="Incorrect proprietary period units given"):
        public_date = calculate_public_date(file_date, "1 centon")


def test_calculate_public_date():

    # Test the basics, with singular and plural units, mixed case
    file_date = date(2023, 1, 10)
    public_date = calculate_public_date(file_date, "1 year")
    assert public_date == date(2024,1,10)

    public_date = calculate_public_date(file_date, "3 Years")
    assert public_date == date(2026,1,10)

    public_date = calculate_public_date(file_date, "1 month")
    assert public_date == date(2023,2,10)

    public_date = calculate_public_date(file_date, "6 Months")
    assert public_date == date(2023,7,10)

    public_date = calculate_public_date(file_date, "28 months")
    assert public_date == date(2025,5,10)

    public_date = calculate_public_date(file_date, "1 Day")
    assert public_date == date(2023,1,11)

    public_date = calculate_public_date(file_date, "30 days")
    assert public_date == date(2023,2,9)


    # Test around leap year and days past the end of month
    file_date = date(2023,2,28)
    
    public_date = calculate_public_date(file_date, "1 day")
    assert public_date == date(2023,3,1)

    file_date = date(2024,2,28)

    public_date = calculate_public_date(file_date, "1 day")
    assert public_date == date(2024,2,29)

    public_date = calculate_public_date(file_date, "2 Days")
    assert public_date == date(2024,3,1)

    file_date = date(2024,2,29)
    public_date = calculate_public_date(file_date, "1 Year")
    assert public_date == date(2025,3,1)

    public_date = calculate_public_date(file_date, "4 years")
    assert public_date == date(2028,2,29)

    file_date = date(2024,1,31)
    public_date = calculate_public_date(file_date, "1 Month")
    assert public_date == date(2024,3,1)
    
    public_date = calculate_public_date(file_date, "2 months")
    assert public_date == date(2024,3,31)

    file_date = date(2024,12,31)
    public_date = calculate_public_date(file_date, "1 month")
    assert public_date == date(2025,1,31)
    
    public_date = calculate_public_date(file_date, "13 months")
    assert public_date == date(2026,1,31)

    public_date = calculate_public_date(file_date, "1 day")
    assert public_date == date(2025,1,1)
