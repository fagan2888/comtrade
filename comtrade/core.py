import os
import warnings
import zipfile
import io

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from .util import KEY_ENV_NAME, KEY_FILE_NAME, DEFAULT_API_URL


class QueryError(Exception):
    def __init__(self, msg, response):
        super(QueryError, self).__init__(msg)
        self.response = response


class ComtradeResult(object):
    def __init__(self, validation, dataset, url):
        self.validation = validation
        self.df = pd.DataFrame(dataset)
        self.url = url


class Comtrade(object):
    def __init__(self, url=DEFAULT_API_URL, token=None, max_retries=3):
        self.url = url
        self.token = None

        if token is None:
            if KEY_ENV_NAME in os.environ:
                self.token = os.environ[KEY_ENV_NAME]
            elif os.path.isfile(KEY_FILE_NAME):
                with open(KEY_FILE_NAME, "r") as f:
                    self.token = f.read().strip()
            else:
                msg = "Comtrade token not detected, usage may be limited."
                warnings.warn(msg)
                return

        if self.token is not None:
            key_len = 152
            if len(self.token) > key_len:
                self.token = token[:key_len]
                m = f"API token too long, using first {key_len} characters"
                warnings.warn(m)
            elif len(self.token) < key_len:
                m = f"API token {token} too short. Should be {key_len} chars"
                raise ValueError(m)

        self.sess = requests.Session()
        self.sess.mount(self.url, HTTPAdapter(max_retries=max_retries))

    def _make_request(self, method, **params):
        url = self.url + method
        r = self.sess.get(url, params=params)
        if r.status_code != 200:
            msg = f"Query failed with status code {r.status_code}. "
            msg += f"Response from server was\n{r.content}"
            raise QueryError(msg, r)

        return r

    def _validate_kwargs(self, method, allowed_kwargs, kwargs):
        for k in kwargs.keys():
            if k not in allowed_kwargs:
                m = f"Argument {k} not allowed for method {method}"
                raise ValueError(m)

    def _validation_dataset_response(self, r):
        js = r.json()
        if "validation" not in js:
            m = "Query indicates success, but doesn't contain validation"
            raise QueryError(m, r)

        if "dataset" not in js:
            m = "Query indicates success, but doesn't contain dataset"
            raise QueryError(m, r)

        if not js["dataset"]:
            m = "Query indicates success, but the dataset is empty"
            raise QueryError(m, r)

        return ComtradeResult(js["validation"], js["dataset"], r.url)

    def get_subuser_token(self, email):
        """
        To get authentication token when the request is originated from
        registered IP range. Email parameters required

        Parameters
        ----------
        email: str
            The email address associated with your UN Comtrade account

        Returns
        -------
        token: str
            The unique token associated with your IP adddres + email pair

        """
        r = self._make_request("getSubUserToken", email=email)
        js = r.json()
        if 'token' in js:
            return js["token"]
        else:
            m = "Query failed to return a valid token"
            raise QueryError(m, r)

    def get_auth_token(self, username, password):
        """
        To get authenticated and get authentication token to be used for
        subsequent API calls

        Parameters
        ----------
        username, password: str
            The username and password for your comtrade account

        Returns
        -------
        token: str
            The unique token associated with your username and password

        """
        r = self._make_request("getAuthToken",
                               username=username, password=password)
        js = r.json()
        if 'token' in js:
            return js["token"]
        else:
            m = "Query failed to return a valid token"
            raise QueryError(m, r)

    def get_user_info(self, token=None):
        """
        To get authenticated using the token

        Parameters
        ----------
        token: str (default=self.token)
            The token for which user info should be reported

        Returns
        -------
        info: dict
            A dict containing information about the user and his/her current
            connection
        """
        token = self.token if token is None else token
        if token is not None:
            return self._make_request("getUserInfo", token=token).json()
        else:
            raise ValueError("Cannot get user info without a token")

    def get(self, **kwargs):
        """
        Get trade data

        Parameters
        ----------
        r: str, optional (default="0")
            Reporting area. The area that reported the trade to UNSD. See `list
            of valid reporters`_

        px: str, optional (default -- see below)
            Trade data classification scheme.  Default values are HS for goods
            and EB02 for services. See list of valid classifications:

            - `HS Harmonized System`_, as reported (e.g. if data was originally
              submitted to UN Comtrade in HS1996 then HS1996 is displayed)
            - `H0 HS 1992`_
            - `H1 HS 1996`_
            - `H2 HS 2002`_
            - `H3 HS 2007`_
            - `H4 HS 2012`_
            - `ST Standard International Trade Classification`_ , as reported
              (e.g. if data was originally submitted to UN Comtrade in SITC
              Rev. 1 then SITC Rev. 1 is displayed)
            - `S1 SITC Revision 1`_
            - `S2 SITC Revision 2`_
            - `S3 SITC Revision 3`_
            - `S4 SITC Revision 4`_
            - `BEC Broad Economic Categories`_
            - `EB02 Extended Balance of Payments Services Classification`_

        ps: str, optional (default="now")
            Time period. Depending on freq, time period can take either
            ``YYYY`` or ``YYYYMM`` or ``now`` or ``recent``. If ``freq`` is
            ``M`` and the form ``YYYY`` is given for this argument data will be
            returned for all months in that year. ``now`` is the most recent
            availble time period. ``recent`` is the 5 most recent available
            time periods.

        p: str, optional(default="all")
            Partner area. The area receiving the trade, based on the reporting
            areas data. See `list of valid partners`_

        rg: str, optional(default="all")
            Trade regime / trade flow. The most common area 1 (imports) and 2
            (exports), see `list of valid trade flows`_

        cc: str, optional(default="AG2")
            Classification code.  commodity code valid in the selected
            classification. Full lists of codes for each classification are
            linked to above under the px parameter. Some codes are valid in all
            classifications:

            - ``TOTAL``: Total trade between reporter and partner, no detail
              breakdown.
            - ``AG1``, ``AG2``, ``AG3``, ``AG4``, ``AG5``, ``AG6``: Detailed
              codes at a specific digit level. For instance AG6 in HS gives all
              of the 6-digit codes, which are the most detailed codes that are
              internationally comparable. Not all classifications have all
              digit levels available. See the classification specific codes for
              more information.
            - ``ALL``: All codes in the classification.

        max: integer, optional(default=500)
            Maximum records returned. A valid integer in the range:

            - Guest: [1, 50000]
            - Authenticated: [1, min(account limit, 250_000)],

            If the number of records returned by the query exceed max results
            are truncated to this number. In output types supporting metadata
            (e.g. json) information about the total number of records that
            would have been returned is included.

            TODO FIGURE THIS OUT ^^

        type: str, optional(default="C")
            Trade data type. Either ``C`` for commodities (merchandise) or
            ``S`` for services

        freq: str, optional (default="A")
            Data set frequency. Allowed values are ``A`` for annual and
            ``M`` for monthly

        head: str, optional(default="H")
            Heading style. Changes the heading line (first line) in CSV output.
            Will also change Excel output once that is available. Valid values:

            - ``H`` Human readable headings, meant to be easy to understand.
              May contain special characters and spaces.
            - ``M`` Machine readable headings that match the JSON output, meant
              to be easy to parse. Does not contain special characters, spaces,
              etc.

        token: str, optional(default=self.token)
            Authorization code

        imts: str, optional(default=2010)
            IMTS format Data fields/columns based on IMTS Concepts &
            Definitions

            - 2010[forthcoming] data that comply with IMTS 2010 Concepts &
              Definitions (addition of mode of transports, 2nd partner country,
              customs procedure codes, imports FOB, if applicable)
            - orig data that comply with earlier version of IMTS Concepts &
              Definitions

        """
        allowed_kwargs = ["r", "px", "ps", "p", "rg", "cc", "max", "type",
                          "freq", "head", "token", "imts"]

        self._validate_kwargs("get", allowed_kwargs, kwargs)
        r = self._make_request("get", **kwargs)
        return self._validation_dataset_response(r)

    def view(self, **kwargs):
        """
        Get trade data

        Parameters
        ----------
        r: str, optional (default="0")
            Reporting area. The area that reported the trade to UNSD. See `list
            of valid reporters`_

        px: str, optional (default -- see below)
            Trade data classification scheme.  Default values are HS for goods
            and EB02 for services. See list of valid classifications:

            - `HS Harmonized System`_, as reported (e.g. if data was originally
              submitted to UN Comtrade in HS1996 then HS1996 is displayed)
            - `H0 HS 1992`_
            - `H1 HS 1996`_
            - `H2 HS 2002`_
            - `H3 HS 2007`_
            - `H4 HS 2012`_
            - `ST Standard International Trade Classification`_ , as reported
              (e.g. if data was originally submitted to UN Comtrade in SITC
              Rev. 1 then SITC Rev. 1 is displayed)
            - `S1 SITC Revision 1`_
            - `S2 SITC Revision 2`_
            - `S3 SITC Revision 3`_
            - `S4 SITC Revision 4`_
            - `BEC Broad Economic Categories`_
            - `EB02 Extended Balance of Payments Services Classification`_

        ps: str, optional (default="now")
            Time period. Depending on freq, time period can take either
            ``YYYY`` or ``YYYYMM`` or ``now`` or ``recent``. If ``freq`` is
            ``M`` and the form ``YYYY`` is given for this argument data will be
            returned for all months in that year. ``now`` is the most recent
            availble time period. ``recent`` is the 5 most recent available
            time periods.

        type: str, optional(default="C")
            Trade data type. Either ``C`` for commodities (merchandise) or
            ``S`` for services

        freq: str, optional (default="A")
            Data set frequency. Allowed values are ``A`` for annual and
            ``M`` for monthly

        token: str, optional(default=self.token)
            Authorization code
        """
        allowed_kwargs = ["r", "px", "ps", "type", "freq", "token"]

        self._validate_kwargs("view", allowed_kwargs, kwargs)
        r = self._make_request("refs/da/view", **kwargs)
        return self._validation_dataset_response(r)

    def get_bulk(self, type, freq, ps, r, px, token=None):
        """
        Get trade data

        Parameters
        ----------
        r: str
            Reporting area. The area that reported the trade to UNSD. See `list
            of valid reporters`_

        px: str
            Trade data classification scheme.  Default values are HS for goods
            and EB02 for services. See list of valid classifications:

            - `HS Harmonized System`_, as reported (e.g. if data was originally
              submitted to UN Comtrade in HS1996 then HS1996 is displayed)
            - `H0 HS 1992`_
            - `H1 HS 1996`_
            - `H2 HS 2002`_
            - `H3 HS 2007`_
            - `H4 HS 2012`_
            - `ST Standard International Trade Classification`_ , as reported
              (e.g. if data was originally submitted to UN Comtrade in SITC
              Rev. 1 then SITC Rev. 1 is displayed)
            - `S1 SITC Revision 1`_
            - `S2 SITC Revision 2`_
            - `S3 SITC Revision 3`_
            - `S4 SITC Revision 4`_
            - `BEC Broad Economic Categories`_
            - `EB02 Extended Balance of Payments Services Classification`_

        ps: str
            Time period. Depending on freq, time period can take either
            ``YYYY`` or ``YYYYMM`` or ``now`` or ``recent``. If ``freq`` is
            ``M`` and the form ``YYYY`` is given for this argument data will be
            returned for all months in that year. ``now`` is the most recent
            availble time period. ``recent`` is the 5 most recent available
            time periods.

        type: str
            Trade data type. Either ``C`` for commodities (merchandise) or
            ``S`` for services

        freq: str
            Data set frequency. Allowed values are ``A`` for annual and
            ``M`` for monthly

        token: str, optional(default=self.token)
            Authorization code

        """
        allowed_kwargs = ["r", "px", "ps", "type", "freq", "token"]
        token = token if token is not None else self.token
        kwargs = dict(type=type, freq=freq, ps=ps, r=r, px=px, token=token)

        self._validate_kwargs("get_bulk", allowed_kwargs, kwargs)
        r = self._make_request(f"get/bulk/{type}/{freq}/{ps}/{r}/{px}",
                               token=token)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        df = pd.read_csv(zf.open(zf.namelist()[0]))
        return ComtradeResult({}, df, r.url)

    def view_bulk(self, **kwargs):
        """
        Get trade data

        Parameters
        ----------
        r: str
            Reporting area. The area that reported the trade to UNSD. See `list
            of valid reporters`_

        px: str
            Trade data classification scheme.  Default values are HS for goods
            and EB02 for services. See list of valid classifications:

            - `HS Harmonized System`_, as reported (e.g. if data was originally
              submitted to UN Comtrade in HS1996 then HS1996 is displayed)
            - `H0 HS 1992`_
            - `H1 HS 1996`_
            - `H2 HS 2002`_
            - `H3 HS 2007`_
            - `H4 HS 2012`_
            - `ST Standard International Trade Classification`_ , as reported
              (e.g. if data was originally submitted to UN Comtrade in SITC
              Rev. 1 then SITC Rev. 1 is displayed)
            - `S1 SITC Revision 1`_
            - `S2 SITC Revision 2`_
            - `S3 SITC Revision 3`_
            - `S4 SITC Revision 4`_
            - `BEC Broad Economic Categories`_
            - `EB02 Extended Balance of Payments Services Classification`_

        ps: str
            Time period. Depending on freq, time period can take either
            ``YYYY`` or ``YYYYMM`` or ``now`` or ``recent``. If ``freq`` is
            ``M`` and the form ``YYYY`` is given for this argument data will be
            returned for all months in that year. ``now`` is the most recent
            availble time period. ``recent`` is the 5 most recent available
            time periods.

        type: str
            Trade data type. Either ``C`` for commodities (merchandise) or
            ``S`` for services

        freq: str
            Data set frequency. Allowed values are ``A`` for annual and
            ``M`` for monthly

        from: str
            Published date from

        token: str, optional(default=self.token)
            Authorization code

        """
        allowed_kwargs = ["r", "px", "ps", "type", "freq", "from", "token"]

        self._validate_kwargs("view", allowed_kwargs, kwargs)
        r = self._make_request("refs/da/bulk", **kwargs)
        return self._validation_dataset_response(r)

    # HELPER METHODS!
    def save_subuser_token(self, email):
        token = self.get_subuser_token(email)
        with open(KEY_FILE_NAME, "w") as f:
            f.write(token)

        print(f"Token saved to {KEY_FILE_NAME}")


_DOC_LINKS = """

        .. _list of valid reporters:
            https://comtrade.un.org/data/cache/reporterAreas.json

        .. _list of valid partners:
            https://comtrade.un.org/data/cache/partnerAreas.json

        .. _list of valid trade flows:
            https://comtrade.un.org/data/cache/tradeRegimes.json

        .. _HS Harmonized System:
            https://comtrade.un.org/data/cache/classificationHS.json

        .. _H0 HS 1992:
            https://comtrade.un.org/data/cache/classificationH0.json

        .. _H1 HS 1996:
            https://comtrade.un.org/data/cache/classificationH1.json

        .. _H2 HS 2002:
            https://comtrade.un.org/data/cache/classificationH2.json

        .. _H3 HS 2007:
            https://comtrade.un.org/data/cache/classificationH3.json

        .. _H4 HS 2012:
            https://comtrade.un.org/data/cache/classificationH4.json

        .. _ST Standard International Trade Classification:
            https://comtrade.un.org/data/cache/classificationST.json

        .. _S1 SITC Revision 1:
            https://comtrade.un.org/data/cache/classificationS1.json

        .. _S2 SITC Revision 2:
            https://comtrade.un.org/data/cache/classificationS2.json

        .. _S3 SITC Revision 3:
            https://comtrade.un.org/data/cache/classificationS3.json

        .. _S4 SITC Revision 4:
            https://comtrade.un.org/data/cache/classificationS4.json

        .. _BEC Broad Economic Categories:
            https://comtrade.un.org/data/cache/classificationBEC.json

        .. _EB02 Extended Balance of Payments Services Classification:
            https://comtrade.un.org/data/cache/classificationEB02.json

"""

Comtrade.view.__doc__ = Comtrade.view.__doc__ + _DOC_LINKS
Comtrade.get.__doc__ = Comtrade.get.__doc__ + _DOC_LINKS
Comtrade.view_bulk.__doc__ = Comtrade.view_bulk.__doc__ + _DOC_LINKS
Comtrade.get_bulk.__doc__ = Comtrade.get_bulk.__doc__ + _DOC_LINKS
