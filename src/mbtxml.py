#    meinBT
#    Copyright (C) 2017  Carine Dengler
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""
:synopsis: XML file parser for the biographical data of the Members
of the German Bundestag (`<https://www.bundestag.de/blob/472878/c1a07a64c9ea8c687df6634f2d9d805b/mdb-stammdaten-data.zip>`_).
"""


# standard library imports
import re
import sys
import logging
import datetime
import multiprocessing

# third party imports
import bs4

# library specific imports


def _get_datetime(date):
    """Get datetime object.

    :param str date: date (DD.MM.YYYY)

    :returns: date
    :rtype: datetime
    """
    try:
        args = [int(word) for word in date.split(".") if word][::-1]
        if len(args):
            date = datetime.datetime(*args)
    except Exception:
        raise
    return date


class MBTXML(object):
    """meinBT XML file parser.

    :ivar int pid: process ID
    :ivar BeautifulSoup soup: XML file tree
    """

    DATE = re.compile(
        r"(?P<DD>[0-9]{2})\.(?P<MM>[0-9]{2})\.(?P<YY>[0-9]{4})"
    )

    def __init__(self, pid, xml):
        """Initialize meinBT XML file parser.

        :param int pid: process ID
        :param str xml: XML file name
        """
        logger = multiprocessing.get_logger().getChild(__name__)
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(stream=sys.stdout))
        self.pid = pid
        logger.info("worker %d parses XML file %s", self.pid, xml)
        try:
            file_ = open(xml)
            self.soup = bs4.BeautifulSoup(file_, "lxml")
        except OSError:
            logger.exception(
                "worker %d failed to parse XML file", self.pid, xml
            )
        finally:
            file_.close()
        return

    def _find_element(self, name, root=None):
        """Find element.

        :param str name: element name
        :param Tag root: root

        :returns: element
        :rtype: Tag
        """
        try:
            if root:
                element = root.find(name=name)
            else:
                self.soup.find(name=name)
        except Exception:
            raise
        return element

    def _find_all_elements(self, name, root=None):
        """Find all elements.

        :param str name: element name
        :param Tag root: root element

        :returns: elements
        :rtype: ResultSet
        """
        try:
            if root:
                elements = root.find_all(name=name)
            else:
                elements = self.soup.find_all(name=name)
        except Exception:
            raise
        return elements

    def find_version(self):
        """Find version.

        :returns: version
        :rtype: str
        """
        # total number: 1
        try:
            version = self._find_element("version").get_text()
        except Exception:
            raise
        return version

    def _find_id(self, root):
        """Find id.

        :param Tag root: root

        :returns: id
        :rtype: str
        """
        # total number: 1
        try:
            id_ = self._find_element("id", root=root).get_text()
        except Exception:
            raise
        return id_

    def _find_name(self, root):
        """Find name.

        :param Tag root: root

        :returns: name
        :rtype: dict
        """
        try:
            name = {
                "nachname":
                self._find_element("nachname", root=root).get_text(),
                "vorname":
                self._find_element("vorname", root=root).get_text(),
                "ortszusatz":
                self._find_element("ortszusatz", root=root).get_text(),
                "adel":
                self._find_element("adel", root=root).get_text(),
                "praefix":
                self._find_element("praefix", root=root).get_text(),
                "anrede_titel":
                self._find_element("anrede_titel", root=root).get_text(),
                "akad_titel":
                self._find_element("akad_titel", root=root).get_text(),
                "historie_von":
                _get_datetime(
                    self._find_element("historie_von", root=root).get_text()
                ),
                "historie_bis":
                _get_datetime(
                    self._find_element("historie_bis", root=root).get_text()
                )
            }
        except Exception:
            raise
        return name

    def _find_namen(self, root):
        """Find namen.

        :param Tag root: root

        :returns: namen
        :rtype: list
        """
        # total number: 1
        try:
            element = self._find_element("namen", root=root)
            if element:
                root = element
                # total number: 1-
                namen = [
                    self._find_name(element) for element in
                    self._find_all_elements("name", root=root)
                    if element
                ]
            else:
                namen = []
        except Exception:
            raise
        return namen

    def _find_biografische_angaben(self, root):
        """Find biografische_angaben.

        :param Tag root: root

        :returns: biografische_angaben
        :rtype: dict
        """
        # total number: 1
        try:
            element = self._find_element("biografische_angaben", root=root)
            if element:
                biografische_angaben = {
                    "geburtsdatum":
                    _get_datetime(
                        self._find_element(
                            "geburtsdatum", root=element
                        ).get_text()
                    ),
                    "geburtsort":
                    self._find_element("geburtsort", root=element).get_text(),
                    "geburtsland":
                    self._find_element("geburtsland", root=element).get_text(),
                    "sterbedatum":
                    _get_datetime(
                        self._find_element(
                            "sterbedatum", root=element
                        ).get_text()
                    ),
                    "geschlecht":
                    self._find_element("geschlecht", root=element).get_text(),
                    "familienstand":
                    self._find_element(
                        "familienstand", root=element
                    ).get_text().split(","),
                    "religion":
                    self._find_element("religion", root=element).get_text(),
                    "beruf":
                    self._find_element(
                        "beruf", root=element
                    ).get_text().split(","),
                    "partei_kurz":
                    self._find_element("partei_kurz", root=element).get_text(),
                    "vita_kurz":
                    self._find_element("vita_kurz", root=element).get_text(),
                    "veroeffentlichungspflichtiges":
                    self._find_element(
                        "veroeffentlichungspflichtiges", root=element
                    ).get_text()
                }
            else:
                biografische_angaben = {}
        except Exception:
            raise
        return biografische_angaben

    def _find_institution(self, root):
        """Find institution.

        :param Tag root: root

        :returns: institution
        :rtype: dict
        """
        try:
            institution = {
                "insart_lang":
                self._find_element("insart_lang", root=root).get_text(),
                "ins_lang":
                self._find_element("ins_lang", root=root).get_text(),
                "mdbins_von":
                _get_datetime(
                    self._find_element("mdbins_von", root=root).get_text()
                ),
                "mdbins_bis":
                _get_datetime(
                    self._find_element("mdbins_bis", root=root).get_text()
                ),
                "fkt_lang":
                self._find_element("fkt_lang", root=root).get_text(),
                "fktins_von":
                _get_datetime(
                    self._find_element("fktins_von", root=root).get_text()
                ),
                "fktins_bis":
                _get_datetime(
                    self._find_element("fktins_bis", root=root).get_text()
                )
            }
        except Exception:
            raise
        return institution

    def _find_institutionen(self, root):
        """Find institutionen.

        :param Tag root: root

        :returns: institutionen
        :rtype: list
        """
        # total number: 1
        try:
            element = self._find_element("institutionen", root=root)
            if element is not None:
                # total number: 1-
                root = element
                institutionen = [
                    self._find_institution(element) for element in
                    self._find_all_elements("institution", root=element)
                    if element
                ]
            else:
                institutionen = []
        except Exception:
            raise
        return institutionen

    def _find_wahlperiode(self, root):
        """Find wahlperiode.

        :param Tag root: root

        :returns: wahlperiode
        :rtype: dict
        """
        try:
            wahlperiode = {
                "wp": self._find_element("wp", root=root).get_text(),
                "mdbwp_von":
                _get_datetime(
                    self._find_element("mdbwp_von", root=root).get_text()
                ),
                "mdbwp_bis":
                _get_datetime(
                    self._find_element("mdbwp_bis", root=root).get_text()
                ),
                "wkr_nummer":
                self._find_element("wkr_nummer", root=root).get_text(),
                "wkr_name":
                self._find_element("wkr_name", root=root).get_text(),
                "wkr_land":
                self._find_element("wkr_land", root=root).get_text(),
                "liste":
                self._find_element("liste", root=root).get_text(),
                "mandatsart":
                self._find_element("mandatsart", root=root).get_text(),
                "institutionen":
                self._find_institutionen(root)
            }
        except Exception:
            raise
        return wahlperiode

    def _find_wahlperioden(self, root):
        """Find wahlperioden.

        :param Tag root: root

        :returns: wahlperioden
        :rtype: list
        """
        # total number: 1
        try:
            element = self._find_element("wahlperioden", root=root)
            if element:
                # total number: 1-
                root = element
                wahlperioden = [
                    self._find_wahlperiode(element) for element in
                    self._find_all_elements("wahlperiode", root=root)
                    if element
                ]
            else:
                wahlperioden = []
        except Exception:
            raise
        return wahlperioden

    def find_mdb(self):
        """Find mdb.

        :returns: mdb
        :rtype: dict
        """
        # total number: 1-
        try:
            for element in self._find_all_elements("mdb"):
                if element:
                    mdb = {
                        "id":
                        self._find_element("id", root=element).get_text(),
                        "namen": self._find_namen(element),
                        "biografische_angaben":
                        self._find_biografische_angaben(element),
                        "wahlperioden":
                        self._find_wahlperioden(element)
                    }
                else:
                    mdb = {}
                yield mdb
        except Exception:
            raise
