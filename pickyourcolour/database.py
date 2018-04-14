import requests
import os
import tarfile
from typing import Text, Generator
import sqlite3

conf = {
    'url': 'http://xkcd.com/color/colorsurvey.tar.gz',
    'datadir': './data/',
    'tarfile': 'colorsurvey.tar.gz',
    'dbname': 'db.db',
}


def outfile_name() -> str:
    return os.path.join(conf['datadir'], conf['tarfile'])


def shoulddownload() -> bool:
    return not os.path.exists(outfile_name())


def downloadfile():
    r = requests.get(conf['url'], stream=True)
    with open(outfile_name(), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def extracttarfile(tar: tarfile.TarFile, member: tarfile.TarInfo) -> Text:
    """Extract the given member from the tarfile. Return the path of the file
    where we output it. If this isn't a regular file or a link, tarfile module
    will return `None`. In this case return None as the output file name.
    """
    outfilename = os.path.join(conf['datadir'], member.name)
    extractfile = tar.extractfile(member)

    if not extractfile:
        return None
    with open(outfilename, 'wb') as f:
        f.write(extractfile.read())
    return outfilename


def extractarchive() -> Generator[Text, None, None]:
    """Extract all the files in the tar archive."""
    tar = tarfile.open(outfile_name(), "r:gz")
    for member in tar.getmembers():
        extract = extracttarfile(tar, member)
        yield extract


def download_pipeline() -> Generator[Text, None, None]:
    if shoulddownload():
        # This is a time consuming operation on slow internet connections.
        downloadfile()
    return extractarchive()


def form_connection(dbname: Text) -> sqlite3.Connection:
    dbpath = os.path.join(conf['datadir'], dbname)
    if os.path.exists(dbpath):
        os.remove(dbpath)
    return sqlite3.connect(dbpath)


def insert_database(files: Generator[Text, None, None]) -> None:
    for filepath in files:
        insert_data(filepath)


def insert_data(infilepath: Text) -> None:
    """Insert the data in the script from infilepath into the database.
    """
    # the databasename is formed from the name of the textfile.
    dbname = os.path.basename(infilepath).replace('.txt', '.db')
    print("Creating {dbname}".format(dbname=dbname))
    connection = form_connection(dbname)
    with open(infilepath, 'r') as f:
        command = f.read()
        c = connection.cursor()
        c.executescript(command)

    # tidyup
    connection.commit()
    connection.close()


def pipeline() -> None:
    insert_database(download_pipeline())
