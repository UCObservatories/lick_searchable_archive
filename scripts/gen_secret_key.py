"""
Script to generate secret keys suitable for Django.

Arguments: The output file to hold the secret key

The file created will have 600 permissions (read/writable to the owner only).
"""
import secrets
import sys
import os

if __name__ == '__main__':
    # Make sure there's an ouptut file given
    if len(sys.argv) < 2:
        print("Output file required.", file=sys.stderr)
        sys.exit(1)

    # Open the output file with the desired 600 permissions
    fd = os.open(sys.argv[1], flags = os.O_WRONLY | os.O_CREAT, mode=0o600)

    key = secrets.token_urlsafe()
    os.write(fd, bytes(key,"UTF-8"))

    os.close(fd)